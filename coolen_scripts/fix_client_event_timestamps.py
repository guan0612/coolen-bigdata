import mysql.connector
import pandas as pd
import sys
import json
import os
import time
from datetime import datetime
from collections import defaultdict

# 添加應用程式路徑（不要寫死 /app，避免離開 Docker 後無法執行）
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# 導入資料庫配置
from backend.database.config import Config
from backend.database.service_lab import ServiceLAB
from backend.database.service_ncu import ServiceNCU

# ==================== 輸出檔案設定 ====================
# 輸出檔案放在腳本所在目錄（coolen_scripts）
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

print("=" * 80)
print("時間戳記修正腳本（LAB + NCU）")
print("=" * 80)
print(f"開始時間：{datetime.now()}")
print("=" * 80)

def fix_database(db_name, db_config, service_class, target_date=None):
    """修正單一資料庫的時間戳記
    
    Args:
        db_name: 資料庫名稱
        db_config: 資料庫連線設定
        service_class: 資料庫服務類別
        target_date: 目標日期字串 (格式: 'YYYY-MM-DD')，如果為 None 則處理今天的資料
    """
    # 如果沒有指定日期，使用今天
    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"\n{'='*80}")
    print(f"開始處理 {db_name} 資料庫（日期: {target_date}）")
    print(f"{'='*80}")
    
    # 為每個資料庫建立獨立的日誌檔案
    conflict_log_file = os.path.join(SCRIPT_DIR, f'timestamp_conflicts_{db_name.lower()}_{timestamp}.json')
    update_log_file = os.path.join(SCRIPT_DIR, f'timestamp_updates_{db_name.lower()}_{timestamp}.log')
    
    print(f"衝突記錄檔案：{conflict_log_file}")
    print(f"更新記錄檔案：{update_log_file}")
    
try:
        def _connect():
            # 連線層優化：此專案使用的 mysql-connector-python 版本不支援 read_timeout/write_timeout
            # 改用 connection_timeout + 連線後設定 session 的 net_*_timeout + ping(reconnect=True)
            return mysql.connector.connect(
                **db_config,
                autocommit=False,
                connection_timeout=int(os.getenv("MYSQL_CONNECTION_TIMEOUT", "30")),
            )

        def _reconnect(old_conn=None):
            try:
                if old_conn is not None:
                    old_conn.close()
            except Exception:
                pass
            return _connect()

        def _ensure_connection(conn, cursor, verbose=False):
            """
            確保資料庫連線正常，必要時自動重連
            返回 (conn, cursor) - 可能是新的連線和游標
            """
            try:
                # 嘗試 ping（如果連線斷開會自動重連）
                conn.ping(reconnect=True, attempts=3, delay=2)
                # 如果 ping 成功，檢查 cursor 是否還有效
                try:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                except Exception:
                    # cursor 失效，重新建立
                    cursor = conn.cursor()
                    if verbose:
                        print("  🔄 重新建立 cursor")
            except Exception as ping_err:
                # ping 失敗，需要重連
                if verbose:
                    print(f"  ⚠️ 連線檢查失敗，重新連線：{ping_err}")
                try:
                    conn = _reconnect(conn)
                    cursor = conn.cursor()
                    # 重連後重新設定 session timeout
                    _set_session_timeouts(cursor, conn, verbose=verbose)
                    if verbose:
                        print("  ✅ 重新連線成功")
                except Exception as re_err:
                    if verbose:
                        print(f"  ❌ 重新連線失敗：{re_err}")
                    raise
            return conn, cursor

        conn = _connect()
        print(f"\n✅ {db_name} 資料庫連線成功！")
    
    # 確保沒有未提交的事務
    if conn.in_transaction:
        conn.rollback()
    
    cursor = conn.cursor()

        # Session 層優化：提高讀寫逾時（避免大量資料傳輸時被 server 端中斷）
        # 這些是 MySQL server 的 session 變數，不依賴 connector 的 connect() 參數
        def _set_session_timeouts(cursor, conn, verbose=False):
            """設定並驗證 session timeout 參數"""
            try:
                net_read_timeout = int(os.getenv("MYSQL_NET_READ_TIMEOUT", "120"))
                net_write_timeout = int(os.getenv("MYSQL_NET_WRITE_TIMEOUT", "120"))
                wait_timeout = int(os.getenv("MYSQL_WAIT_TIMEOUT", "28800"))
                
                cursor.execute(f"SET SESSION net_read_timeout = {net_read_timeout}")
                cursor.execute(f"SET SESSION net_write_timeout = {net_write_timeout}")
                cursor.execute(f"SET SESSION wait_timeout = {wait_timeout}")
                conn.commit()
                
                # 只在 verbose=True 時驗證並顯示（避免每次查詢都輸出）
                if verbose:
                    cursor.execute("SHOW VARIABLES LIKE 'net_read_timeout'")
                    actual_read = cursor.fetchone()[1]
                    cursor.execute("SHOW VARIABLES LIKE 'net_write_timeout'")
                    actual_write = cursor.fetchone()[1]
                    cursor.execute("SHOW VARIABLES LIKE 'wait_timeout'")
                    actual_wait = cursor.fetchone()[1]
                    
                    if int(actual_read) == net_read_timeout and int(actual_write) == net_write_timeout:
                        print(f"  ✅ Session timeout 設定成功：net_read={actual_read}s, net_write={actual_write}s, wait={actual_wait}s")
                    else:
                        print(f"  ⚠️ Session timeout 設定可能未生效：net_read={actual_read}s (期望 {net_read_timeout}s), net_write={actual_write}s (期望 {net_write_timeout}s)")
                
                return net_read_timeout, net_write_timeout, wait_timeout
            except Exception as e:
                # 不因為設定失敗而中止
                if conn.in_transaction:
                    conn.rollback()
                if verbose:
                    print(f"  ⚠️ 設定 session timeout 失敗：{e}")
                return None, None, None
        
        # 初始設定 session timeout（verbose=True 顯示驗證結果）
        _set_session_timeouts(cursor, conn, verbose=True)
        
        # 建立 baseline 表（記錄每個 server_sign_token 的基準時間）
        # 使用主連線 conn 和 cursor 來建立表，確保在同一連線中執行
        baseline_sql = '''CREATE TABLE IF NOT EXISTS timestamp_fix_baseline (
            server_sign_token VARCHAR(255) PRIMARY KEY,
            baseline_timestamp INT NOT NULL COMMENT '該 token 的歷史最小原始 timestamp',
            server_token_unix INT NOT NULL COMMENT '該 token 的 unix 秒數',
            fixed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_server_token_unix (server_token_unix)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='時間戳記修正基準時間記錄表';'''
        cursor.execute(baseline_sql)
        conn.commit()
        print(f"✅ {db_name} Baseline 表已建立/檢查完成")
        
        # 建立進度表（記錄處理進度，支援斷點續傳）
        progress_sql = '''CREATE TABLE IF NOT EXISTS timestamp_fix_progress (
            db_name VARCHAR(50) PRIMARY KEY COMMENT '資料庫名稱（LAB 或 NCU）',
            last_processed_timestamp INT NOT NULL DEFAULT 0 COMMENT '最後處理到的 timestamp',
            last_processed_batch_num INT NOT NULL DEFAULT 0 COMMENT '最後處理的批次編號',
            total_processed BIGINT NOT NULL DEFAULT 0 COMMENT '總處理記錄數',
            total_updated BIGINT NOT NULL DEFAULT 0 COMMENT '總成功更新記錄數',
            total_conflicts BIGINT NOT NULL DEFAULT 0 COMMENT '總衝突記錄數',
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='時間戳記修正進度記錄表';'''
        cursor.execute(progress_sql)
        conn.commit()
        print(f"✅ {db_name} 進度表已建立/檢查完成")
    
    # 先統計總數
    count_query = """
    SELECT COUNT(*) as total
    FROM client_event
    WHERE server_sign_token IS NOT NULL 
      AND server_sign_token != ''
    """
    cursor.execute(count_query)
    total_count = cursor.fetchone()[0]
        print(f"{db_name} 總記錄數：{total_count:,}")
    
    if total_count == 0:
            print(f"⚠️ {db_name} 沒有需要修正的記錄")
        conn.close()
            return {
                'db_name': db_name,
                'total_processed': 0,
                'total_updated': 0,
                'total_conflicts': 0,
                'conflicts_data': []
            }
    
        # 批次處理設定（避免一次拉太多導致 2013 斷線）
        batch_size = int(os.getenv("FIX_BATCH_SIZE", "10000"))  # 預設 1 萬筆
        min_batch_size = int(os.getenv("FIX_MIN_BATCH_SIZE", "500"))  # 批次切半的下限
    print(f"使用批次處理，每批 {batch_size:,} 筆記錄\n")
    
        # 檢查是否有上次的進度（支援斷點續傳）
        progress_query = """
        SELECT last_processed_timestamp, last_processed_batch_num, 
               total_processed, total_updated, total_conflicts
        FROM timestamp_fix_progress
        WHERE db_name = %s
        """
        cursor.execute(progress_query, (db_name,))
        progress_result = cursor.fetchone()
        
        if progress_result:
            last_timestamp = progress_result[0]
            last_batch_num = progress_result[1]
            total_processed = progress_result[2] or 0
            total_updated = progress_result[3] or 0
            total_conflicts = progress_result[4] or 0
            print(f"📌 發現上次進度：從 timestamp {last_timestamp:,} 繼續（批次 {last_batch_num}）")
            print(f"   已處理：{total_processed:,} 筆，已更新：{total_updated:,} 筆，衝突：{total_conflicts:,} 筆")
        else:
            last_timestamp = 0
            last_batch_num = 0
    total_processed = 0
    total_updated = 0
    total_conflicts = 0
            print(f"🆕 首次執行，從 timestamp 0 開始")
        
        conflicts_data = []
        current_batch_size = batch_size
        batch_num = last_batch_num  # 從上次的批次編號繼續
        
        # 開啟更新記錄檔案（追加模式，保留之前的記錄）
        log_mode = 'a' if progress_result else 'w'
        with open(update_log_file, log_mode, encoding='utf-8') as log_file:
            if not progress_result:
                log_file.write(f"時間戳記修正記錄 - {db_name} - {datetime.now()}\n")
        log_file.write("=" * 80 + "\n\n")
            else:
                log_file.write(f"\n{'='*80}\n")
                log_file.write(f"繼續執行 - {datetime.now()}\n")
                log_file.write(f"從 timestamp {last_timestamp:,} 繼續（批次 {last_batch_num}）\n")
                log_file.write(f"{'='*80}\n\n")
            
            consecutive_query_failures = 0  # 連續查詢失敗次數，避免無限迴圈
            max_consecutive_query_failures = int(os.getenv("FIX_MAX_CONSECUTIVE_QUERY_FAILURES", "50"))  # 增加到 50 次，給更多重試機會

            while True:
                batch_num += 1
                print(f"\n處理第 {batch_num} 批資料（timestamp >= {last_timestamp}）...")
            
                # 確保沒有未提交的事務
            if conn.in_transaction:
                conn.rollback()
            
                # 查詢批次資料（使用範圍查詢，避免 OFFSET 效能問題）
                #
                # 重要：排除「已修正」的紀錄，避免因 client_event_timestamp 被更新後，
                #       仍落在後續範圍而被重複處理。
                # 定義「已修正」：該 server_sign_token 已建立 baseline，且 client_event_timestamp
                #                 已落在 server_token_unix ± 24 小時（±86400 秒）範圍內。
            query = f"""
            SELECT 
                    ce.client_event_timestamp,
                    ce.server_sign_token,
                    ce.action_activity,
                    ce.video_info_currentTime,
                    CAST(SUBSTRING_INDEX(ce.server_sign_token, ':', -1) AS UNSIGNED) AS server_token_unix
                FROM client_event AS ce
                LEFT JOIN timestamp_fix_baseline AS b
                       ON ce.server_sign_token = b.server_sign_token
                WHERE ce.server_sign_token IS NOT NULL 
                  AND ce.server_sign_token != ''
                  AND ce.client_event_timestamp >= {last_timestamp}
                  AND NOT (
                      b.server_sign_token IS NOT NULL
                      AND ce.client_event_timestamp >= (b.server_token_unix - 86400)
                      AND ce.client_event_timestamp <= (b.server_token_unix + 86400)
                  )
                ORDER BY ce.client_event_timestamp
                LIMIT {current_batch_size}
                """
                
                # 讀取批次資料（加上重試 + 斷線自動重連；必要時切半 batch）
                max_retries = 3
                attempt = 0
                df = None
                while True:
                    try:
                        # 確保連線正常（使用統一的連線檢查函數）
                        conn, cursor = _ensure_connection(conn, cursor, verbose=False)
                        
                        # 查詢前重新設定 session timeout（防止被覆蓋，verbose=False 不顯示輸出）
                        try:
                            _set_session_timeouts(cursor, conn, verbose=False)
                        except Exception:
                            pass
            
            df = pd.read_sql(query, conn)
                        consecutive_query_failures = 0
                        break
                    except Exception as e:
                        attempt += 1
                        err_text = str(e)
                        err_type = type(e).__name__
                        
                        # 詳細的錯誤日誌
                        error_details = {
                            'error_type': err_type,
                            'error_message': err_text,
                            'attempt': attempt,
                            'max_retries': max_retries,
                            'batch_num': batch_num,
                            'batch_size': current_batch_size,
                            'last_timestamp': last_timestamp
                        }
                        
                        # 檢查是否為連線相關錯誤
                        is_connection_error = any(keyword in err_text.lower() for keyword in [
                            'lost connection', 'connection', 'timeout', '2013', '2006', 'gone away'
                        ])
                        
                        if is_connection_error:
                            print(f"  ❌ 連線錯誤（attempt {attempt}/{max_retries}）：{err_text}")
                            print(f"     錯誤類型：{err_type}，批次：{batch_num}，batch_size：{current_batch_size:,}")
                            log_file.write(
                                f"批次 {batch_num}: 連線錯誤 - {err_type}: {err_text}\n"
                                f"  詳細資訊：{error_details}\n"
                            )
                        else:
                            print(f"  ❌ 查詢失敗（attempt {attempt}/{max_retries}）：{err_text}")
                            log_file.write(
                                f"批次 {batch_num}: 查詢錯誤 - {err_type}: {err_text}\n"
                                f"  詳細資訊：{error_details}\n"
                            )
                        
                        # 先嘗試重連
                        try:
                            conn = _reconnect(conn)
                            cursor = conn.cursor()
                            # 重連後重新設定 session timeout（verbose=True 顯示驗證結果）
                            _set_session_timeouts(cursor, conn, verbose=True)
                        except Exception as re:
                            print(f"  ❌ 重連失敗：{re}")
                            log_file.write(f"批次 {batch_num}: 重連失敗 - {re}\n")

                        if attempt >= max_retries:
                            # 如果一直失敗，嘗試把 batch 切半後重試
                            if current_batch_size <= min_batch_size:
                                # 不直接讓整個程式結束：記錄後等待，重連並重試同一批
                                consecutive_query_failures += 1
                                log_file.write(
                                    f"批次 {batch_num}: 查詢連續失敗（已到最小 batch_size={current_batch_size}），"
                                    f"等待後重試。錯誤: {err_text}\n"
                                )
                                print(
                                    f"  ⚠️ 已到最小 batch_size={current_batch_size}，仍查詢失敗；"
                                    f"等待後重試（連續失敗 {consecutive_query_failures}/{max_consecutive_query_failures}）"
                                )
                                if consecutive_query_failures >= max_consecutive_query_failures:
                                    raise RuntimeError(
                                        f"連續查詢失敗超過上限 {max_consecutive_query_failures} 次，停止以避免無限迴圈。最後錯誤: {err_text}"
                                    )
                                time.sleep(int(os.getenv("FIX_QUERY_BACKOFF_SECONDS", "10")))
                                attempt = 0
                                # 強制重連後回到 while True 再試一次同一批
                                try:
                                    conn = _reconnect(conn)
                                    cursor = conn.cursor()
                                except Exception:
                                    pass
                                continue
                            new_batch_size = max(min_batch_size, current_batch_size // 2)
                            if new_batch_size == current_batch_size:
                                consecutive_query_failures += 1
                                log_file.write(
                                    f"批次 {batch_num}: 無法再降低 batch_size（仍查詢失敗），等待後重試。錯誤: {err_text}\n"
                                )
                                print(
                                    f"  ⚠️ 無法再降低 batch_size（目前 {current_batch_size}），"
                                    f"等待後重試（連續失敗 {consecutive_query_failures}/{max_consecutive_query_failures}）"
                                )
                                if consecutive_query_failures >= max_consecutive_query_failures:
                                    raise RuntimeError(
                                        f"連續查詢失敗超過上限 {max_consecutive_query_failures} 次，停止以避免無限迴圈。最後錯誤: {err_text}"
                                    )
                                time.sleep(int(os.getenv("FIX_QUERY_BACKOFF_SECONDS", "10")))
                                attempt = 0
                                continue
                            current_batch_size = new_batch_size
                            print(f"  🔧 降低 batch_size → {current_batch_size:,}（避免查詢太大導致斷線）")
                            attempt = 0
                            # 重新生成 query（需與上方邏輯一致：同樣排除已修正紀錄）
                            query = f"""
                            SELECT 
                                ce.client_event_timestamp,
                                ce.server_sign_token,
                                ce.action_activity,
                                ce.video_info_currentTime,
                                CAST(SUBSTRING_INDEX(ce.server_sign_token, ':', -1) AS UNSIGNED) AS server_token_unix
                            FROM client_event AS ce
                            LEFT JOIN timestamp_fix_baseline AS b
                                   ON ce.server_sign_token = b.server_sign_token
                            WHERE ce.server_sign_token IS NOT NULL 
                              AND ce.server_sign_token != ''
                              AND ce.client_event_timestamp >= {last_timestamp}
                              AND NOT (
                                  b.server_sign_token IS NOT NULL
                                  AND ce.client_event_timestamp >= (b.server_token_unix - 86400)
                                  AND ce.client_event_timestamp <= (b.server_token_unix + 86400)
                              )
                            ORDER BY ce.client_event_timestamp
                            LIMIT {current_batch_size}
                            """
                        else:
                            time.sleep(2)
            
            # pandas read_sql 可能會開啟事務，查詢後清理
            if conn.in_transaction:
                conn.rollback()
            
            if df.empty:
                break
            
            print(f"  讀取 {len(df):,} 筆記錄")
            total_processed += len(df)
            
                # 更新 last_timestamp 為這批資料的最大 timestamp（用於下次查詢）
                min_timestamp = int(df['client_event_timestamp'].min())
                max_timestamp = int(df['client_event_timestamp'].max())
                last_timestamp = max_timestamp + 1  # 下次從這個 timestamp + 1 開始
                
                # 以「完整 server_sign_token」分組
            groups = defaultdict(list)
            for idx, row in df.iterrows():
                    server_sign_token = row['server_sign_token']
                server_token_unix = int(row['server_token_unix'])
                    groups[server_sign_token].append({
                    'original_timestamp': int(row['client_event_timestamp']),
                    'server_sign_token': row['server_sign_token'],
                    'action_activity': row['action_activity'],
                    'video_info_currentTime': int(row['video_info_currentTime']),
                        'server_token_unix': server_token_unix,
                    'index': idx
                })
            
            print(f"  分組完成，共 {len(groups):,} 組")
            
                # 優化 1: 批次查詢所有需要的 baseline（避免 N+1 查詢問題）
                all_tokens = list(groups.keys())
                baseline_map = {}
                if all_tokens:
                    # 確保連線正常（查詢 baseline 前檢查）
                    conn, cursor = _ensure_connection(conn, cursor, verbose=False)
                    
                    # 批次查詢 baseline（使用 IN 子句）
                    placeholders = ','.join(['%s'] * len(all_tokens))
                    baseline_query = f"""
                    SELECT server_sign_token, baseline_timestamp, server_token_unix
                    FROM timestamp_fix_baseline
                    WHERE server_sign_token IN ({placeholders})
                    """
                    cursor.execute(baseline_query, all_tokens)
                    for row in cursor.fetchall():
                        baseline_map[row[0]] = {
                            'baseline_timestamp': row[1],
                            'server_token_unix': row[2]
                        }
                
            # 準備更新資料
            update_data = []
            batch_conflicts = []
                baseline_updates = []  # 收集需要更新的 baseline
                baseline_inserts = []  # 收集需要插入的 baseline
                
                for server_sign_token, records in groups.items():
                    # 從批次查詢結果中取得 baseline
                    baseline_result = baseline_map.get(server_sign_token)
                    
                    # 同一個 server_sign_token 內，末段 unix 秒數理論上相同；保險起見取最小值
                    server_token_unix = min(int(r.get('server_token_unix', 0)) for r in records)
                    
                    # 計算本次批次的最小 timestamp
                    current_min_timestamp = min(r['original_timestamp'] for r in records)
                    
                    if baseline_result:
                        # 已有 baseline，使用歷史基準時間
                        baseline_timestamp = baseline_result['baseline_timestamp']
                        baseline_server_token_unix = baseline_result['server_token_unix']
                        # 如果本次資料有更小的 timestamp，更新 baseline
                        if current_min_timestamp < baseline_timestamp:
                            baseline_timestamp = current_min_timestamp
                            # 收集到批次更新列表（優化 4: 批次處理 baseline 更新）
                            baseline_updates.append((baseline_timestamp, server_sign_token))
                            print(f"  Token {server_sign_token[:50]}... 將更新 baseline: {baseline_timestamp} (原本: {baseline_result['baseline_timestamp']})")
                        else:
                            print(f"  Token {server_sign_token[:50]}... 使用既有 baseline: {baseline_timestamp}")
                        # 確保 server_token_unix 一致（如果不一致，記錄警告）
                        if baseline_server_token_unix != server_token_unix:
                            print(f"  ⚠️ 警告：baseline 的 server_token_unix ({baseline_server_token_unix}) 與本次 ({server_token_unix}) 不一致")
                    else:
                        # 沒有 baseline，用本次批次的最小 timestamp 當基準
                        baseline_timestamp = current_min_timestamp
                        # 收集到批次插入列表（優化 4: 批次處理 baseline 插入）
                        baseline_inserts.append((server_sign_token, baseline_timestamp, server_token_unix))
                        print(f"  Token {server_sign_token[:50]}... 將建立新 baseline: {baseline_timestamp}")
                    
                if len(records) == 1:
                    # 只有一筆記錄，直接更新
                    r = records[0]
                        # 計算 time_diff（相對於 baseline）
                        time_diff = r['original_timestamp'] - baseline_timestamp
                        new_timestamp = server_token_unix + time_diff
                    update_data.append({
                        'original_timestamp': r['original_timestamp'],
                        'new_timestamp': new_timestamp,
                        'server_sign_token': r['server_sign_token'],
                        'action_activity': r['action_activity'],
                            'video_info_currentTime': r['video_info_currentTime'],
                            'server_token_unix': server_token_unix,
                            'baseline_timestamp': baseline_timestamp
                    })
                    continue
                
                    # 多筆記錄：使用 baseline 當基準
                base_timestamp = server_token_unix
                
                    # 計算每筆記錄修正後的值（time_diff 相對於 baseline_timestamp）
                updated_records = []
                for r in records:
                        time_diff = r['original_timestamp'] - baseline_timestamp
                    new_timestamp = base_timestamp + time_diff
                    updated_records.append({
                        'original_timestamp': r['original_timestamp'],
                        'new_timestamp': new_timestamp,
                        'server_sign_token': r['server_sign_token'],
                        'action_activity': r['action_activity'],
                        'video_info_currentTime': r['video_info_currentTime'],
                            'time_diff': time_diff,
                            'server_token_unix': server_token_unix,
                            'baseline_timestamp': baseline_timestamp
                    })
                
                # 檢查主鍵衝突
                conflict_keys = defaultdict(list)
                for r in updated_records:
                    key = (r['new_timestamp'], r['server_sign_token'], r['action_activity'], r['video_info_currentTime'])
                    conflict_keys[key].append(r)
                
                # 處理衝突和更新
                for key, records_list in conflict_keys.items():
                    if len(records_list) > 1:
                            # 有衝突，將所有衝突記錄的 new_timestamp 設為 baseline 時間（server_token_unix）
                            conflict_resolved_count = 0
                            for r in records_list:
                                original_new_timestamp = r['new_timestamp']
                                r['new_timestamp'] = r['server_token_unix']  # 使用 baseline 時間
                                r['conflict_resolved'] = True
                                r['original_new_timestamp'] = original_new_timestamp
                                conflict_resolved_count += 1
                            
                            # 記錄衝突資訊
                        conflict_info = {
                                'server_sign_token': server_sign_token,
                                'server_token_unix': base_timestamp,
                            'conflict_key': {
                                'timestamp': key[0],
                                'server_sign_token': key[1],
                                'action_activity': key[2],
                                'video_info_currentTime': key[3]
                            },
                            'conflict_records': records_list,
                                'conflict_count': len(records_list),
                                'conflict_resolved': True,
                                'resolution_method': 'baseline_time'
                        }
                        batch_conflicts.append(conflict_info)
                        total_conflicts += len(records_list)
                        
                            # 記錄到日誌
                            log_file.write(f"⚠️ 主鍵衝突（已解決：使用 baseline 時間）：\n")
                            log_file.write(f"  server_sign_token: {server_sign_token}\n")
                            log_file.write(f"  server_token_unix: {base_timestamp}\n")
                        log_file.write(f"  衝突主鍵: timestamp={key[0]}, activity={key[2]}, currentTime={key[3]}\n")
                        log_file.write(f"  衝突記錄數: {len(records_list)}\n")
                        for i, r in enumerate(records_list, 1):
                                log_file.write(f"    記錄 {i}: 原始 timestamp={r['original_timestamp']}, 原新 timestamp={r.get('original_new_timestamp', r['new_timestamp'])}, 解決後 timestamp={r['new_timestamp']} (baseline), 時間差={r['time_diff']}秒\n")
                        log_file.write("\n")
                            
                            # 將解決後的記錄加入更新列表
                            update_data.extend(records_list)
                    else:
                        # 沒有衝突，加入更新列表
                        update_data.append(records_list[0])
            
                # 優化 4: 批次處理 baseline 更新和插入
                if baseline_updates:
                    try:
                        # 確保連線正常
                        conn, cursor = _ensure_connection(conn, cursor, verbose=False)
                        
                        update_baseline_query = """
                        UPDATE timestamp_fix_baseline
                        SET baseline_timestamp = %s, updated_at = NOW()
                        WHERE server_sign_token = %s
                        """
                        cursor.executemany(update_baseline_query, baseline_updates)
                        conn.commit()
                        print(f"  ✅ 批次更新 {len(baseline_updates):,} 個 baseline")
                    except Exception as e:
                        conn.rollback()
                        print(f"  ⚠️ 批次更新 baseline 失敗: {e}")
                
                if baseline_inserts:
                    try:
                        # 確保連線正常
                        conn, cursor = _ensure_connection(conn, cursor, verbose=False)
                        
                        insert_baseline_query = """
                        INSERT INTO timestamp_fix_baseline 
                        (server_sign_token, baseline_timestamp, server_token_unix, fixed_at)
                        VALUES (%s, %s, %s, NOW())
                        """
                        cursor.executemany(insert_baseline_query, baseline_inserts)
                        conn.commit()
                        print(f"  ✅ 批次插入 {len(baseline_inserts):,} 個 baseline")
                    except Exception as e:
                        conn.rollback()
                        print(f"  ⚠️ 批次插入 baseline 失敗: {e}")
                
                # 批次更新（優化 2: 使用臨時表 + JOIN 批量更新）
            if update_data:
                    print(f"  準備更新 {len(update_data):,} 筆記錄（使用臨時表 + JOIN）...")
                
                try:
                    # 確保連線正常
                    conn, cursor = _ensure_connection(conn, cursor, verbose=False)

                    # 確保沒有未提交的事務
                    if conn.in_transaction:
                        conn.rollback()
                    
                    # 步驟 1: 檢查資料庫中已存在的主鍵（避免 UPDATE 時衝突）
                    # 檢查新的主鍵（new_timestamp, server_sign_token, action_activity, video_info_currentTime）
                    # 是否已經存在於資料庫中。如果存在，UPDATE 會失敗，所以我們先過濾掉這些記錄。
                    existing_keys_query = """
                    SELECT DISTINCT 
                        client_event_timestamp,
                        server_sign_token,
                        action_activity,
                        video_info_currentTime
                    FROM client_event
                    WHERE (client_event_timestamp, server_sign_token, action_activity, video_info_currentTime) IN (
                        {placeholders}
                    )
                    """
                    # 準備查詢參數：要檢查的新主鍵
                    check_keys = [
                        (data['new_timestamp'], data['server_sign_token'],
                         data['action_activity'], data['video_info_currentTime'])
                        for data in update_data
                    ]

                    existing_keys_set = set()
                    db_conflict_records = []
                    safe_update_data = []

                    if check_keys:
                        # 分批檢查（避免 IN 子句過大）
                        check_batch_size = 5000
                        for i in range(0, len(check_keys), check_batch_size):
                            batch_keys = check_keys[i:i + check_batch_size]
                            placeholders = ','.join(['(%s, %s, %s, %s)'] * len(batch_keys))
                            query = existing_keys_query.format(placeholders=placeholders)
                            params = [item for sublist in batch_keys for item in sublist]

                            # 確保連線正常（每批檢查前都驗證）
                            conn, cursor = _ensure_connection(conn, cursor, verbose=False)

                            cursor.execute(query, params)
                            existing_results = cursor.fetchall()
                            for row in existing_results:
                                existing_keys_set.add(tuple(row))

                        # 過濾出會衝突的記錄，並嘗試解決衝突
                    for data in update_data:
                            key = (data['new_timestamp'], data['server_sign_token'],
                                   data['action_activity'], data['video_info_currentTime'])
                            if key in existing_keys_set:
                                # 這個主鍵已存在，嘗試使用 baseline 時間解決衝突
                                server_token_unix = data.get('server_token_unix')
                                if server_token_unix:
                                    # 使用 baseline 時間（server_token_unix）作為 new_timestamp
                                    original_new_timestamp = data['new_timestamp']
                                    data['new_timestamp'] = server_token_unix
                                    data['conflict_resolved'] = True
                                    data['original_new_timestamp'] = original_new_timestamp

                                    # 重新檢查是否還有衝突
                                    new_key = (data['new_timestamp'], data['server_sign_token'],
                                               data['action_activity'], data['video_info_currentTime'])
                                    if new_key in existing_keys_set:
                                        # 即使使用 baseline 時間仍然衝突，嘗試逐步增加時間
                                        max_attempts = 100  # 最多嘗試100次（增加100秒）
                                        attempt = 0
                                        resolved = False
                                        baseline_timestamp = data['new_timestamp']  # 保存 baseline 時間

                                        while attempt < max_attempts:
                                            attempt += 1
                                            data['new_timestamp'] = baseline_timestamp + attempt  # 每次增加1秒
                                            new_key = (data['new_timestamp'], data['server_sign_token'],
                                                       data['action_activity'], data['video_info_currentTime'])

                                            if new_key not in existing_keys_set:
                                                # 找到不衝突的時間
                                                resolved = True
                                                data['conflict_resolved'] = True
                                                data['time_offset'] = attempt  # 記錄增加了多少秒
                                                safe_update_data.append(data)
                                                log_file.write(f"✅ 資料庫主鍵衝突已解決（使用 baseline + {attempt} 秒）：\n")
                                                log_file.write(f"  server_sign_token: {data['server_sign_token']}\n")
                                                log_file.write(
                                                    f"  原始 timestamp: {data['original_timestamp']}, 原新 timestamp: {original_new_timestamp}, "
                                                    f"baseline timestamp: {baseline_timestamp}, 解決後 timestamp: {data['new_timestamp']}\n\n"
                                                )
                                                break

                                        if not resolved:
                                            # 即使增加100秒仍然衝突，記錄並跳過
                                            data['new_timestamp'] = baseline_timestamp  # 恢復為 baseline 時間
                                            db_conflict_records.append(data)
                                            total_conflicts += 1

                                            conflict_info = {
                                                'server_sign_token': data['server_sign_token'],
                                                'server_token_unix': server_token_unix,
                                                'conflict_key': {
                                                    'timestamp': new_key[0],
                                                    'server_sign_token': new_key[1],
                                                    'action_activity': new_key[2],
                                                    'video_info_currentTime': new_key[3]
                                                },
                                                'conflict_records': [{
                                                    'original_timestamp': data['original_timestamp'],
                                                    'new_timestamp': data['new_timestamp'],
                                                    'original_new_timestamp': original_new_timestamp,
                                                    'server_sign_token': data['server_sign_token'],
                                                    'action_activity': data['action_activity'],
                                                    'video_info_currentTime': data['video_info_currentTime'],
                                                    'time_diff': data.get('time_diff', None)
                                                }],
                                                'conflict_count': 1,
                                                'conflict_type': 'database_existing_key_after_baseline_and_offset',
                                                'conflict_resolved': False,
                                                'max_attempts': max_attempts
                                            }
                                            batch_conflicts.append(conflict_info)

                                            log_file.write(
                                                f"⚠️ 資料庫主鍵衝突（使用 baseline + 最多 {max_attempts} 秒後仍衝突，跳過更新）：\n"
                                            )
                                            log_file.write(f"  server_sign_token: {data['server_sign_token']}\n")
                                            log_file.write(
                                                f"  衝突主鍵: timestamp={new_key[0]}, activity={new_key[2]}, currentTime={new_key[3]}\n"
                                            )
                                            log_file.write(
                                                f"  原始 timestamp: {data['original_timestamp']}, 原新 timestamp: {original_new_timestamp}, "
                                                f"baseline timestamp: {baseline_timestamp}\n\n"
                                            )
                                    else:
                                        # 使用 baseline 時間後解決了衝突，可以更新
                                        safe_update_data.append(data)
                                        log_file.write(f"✅ 資料庫主鍵衝突已解決（使用 baseline 時間）：\n")
                                        log_file.write(f"  server_sign_token: {data['server_sign_token']}\n")
                                        log_file.write(
                                            f"  原始 timestamp: {data['original_timestamp']}, 原新 timestamp: {original_new_timestamp}, "
                                            f"解決後 timestamp: {data['new_timestamp']} (baseline)\n\n"
                                        )
                                else:
                                    # 沒有 server_token_unix，無法解決，記錄為衝突
                                    db_conflict_records.append(data)
                                    total_conflicts += 1

                                    conflict_info = {
                                        'server_sign_token': data['server_sign_token'],
                                        'server_token_unix': None,
                                        'conflict_key': {
                                            'timestamp': key[0],
                                            'server_sign_token': key[1],
                                            'action_activity': key[2],
                                            'video_info_currentTime': key[3]
                                        },
                                        'conflict_records': [{
                                            'original_timestamp': data['original_timestamp'],
                                            'new_timestamp': data['new_timestamp'],
                                            'server_sign_token': data['server_sign_token'],
                                            'action_activity': data['action_activity'],
                                            'video_info_currentTime': data['video_info_currentTime'],
                                            'time_diff': data.get('time_diff', None)
                                        }],
                                        'conflict_count': 1,
                                        'conflict_type': 'database_existing_key_no_baseline',
                                        'conflict_resolved': False
                                    }
                                    batch_conflicts.append(conflict_info)

                                    log_file.write(f"⚠️ 資料庫主鍵衝突（無 baseline 資訊，跳過更新）：\n")
                                    log_file.write(f"  server_sign_token: {data['server_sign_token']}\n")
                                    log_file.write(
                                        f"  衝突主鍵: timestamp={key[0]}, activity={key[2]}, currentTime={key[3]}\n"
                                    )
                                    log_file.write(
                                        f"  原始 timestamp: {data['original_timestamp']}, 新 timestamp: {data['new_timestamp']}\n\n"
                                    )
                            else:
                                # 不會衝突，可以更新
                                safe_update_data.append(data)
                    else:
                        # 沒有要檢查的 key（通常代表 update_data 為空），直接保留原本資料
                        safe_update_data = list(update_data)

                    if db_conflict_records:
                        print(f"  ⚠️ 發現 {len(db_conflict_records):,} 筆資料庫層級主鍵衝突（已過濾）")

                    # 只更新不會衝突的記錄
                    update_data = safe_update_data

                    if not update_data:
                        print(f"  ⚠️ 所有記錄都有衝突，跳過此批次更新")
                        log_file.write(f"批次 {batch_num}: 所有記錄都有衝突，跳過更新\n")
                    else:
                        # 確保連線正常（開始更新前再次檢查）
                        conn, cursor = _ensure_connection(conn, cursor, verbose=False)

                        # 步驟 2: 建立臨時表
                        cursor.execute("""
                            CREATE TEMPORARY TABLE IF NOT EXISTS temp_timestamp_updates (
                                old_timestamp INT NOT NULL,
                                new_timestamp INT NOT NULL,
                                server_sign_token VARCHAR(100) NOT NULL,
                                action_activity VARCHAR(50) NOT NULL,
                                video_info_currentTime INT NOT NULL,
                                PRIMARY KEY (old_timestamp, server_sign_token, action_activity, video_info_currentTime)
                            ) ENGINE=InnoDB
                        """)

                        # 步驟 3: 批次插入需要更新的資料到臨時表
                        insert_values = [
                            (
                                data['original_timestamp'],
                            data['new_timestamp'],
                            data['server_sign_token'],
                            data['action_activity'],
                            data['video_info_currentTime']
                            )
                            for data in update_data
                        ]
                        cursor.executemany("""
                            INSERT INTO temp_timestamp_updates 
                            (old_timestamp, new_timestamp, server_sign_token, action_activity, video_info_currentTime)
                            VALUES (%s, %s, %s, %s, %s)
                        """, insert_values)
                    
                        # 步驟 4: 一次性 JOIN 更新（這是關鍵優化！）
                        # 更新前再次確保連線正常（這是最關鍵的操作）
                        conn, cursor = _ensure_connection(conn, cursor, verbose=False)

                        cursor.execute("""
                            UPDATE client_event ce
                            INNER JOIN temp_timestamp_updates tu ON 
                                ce.client_event_timestamp = tu.old_timestamp
                                AND ce.server_sign_token = tu.server_sign_token
                                AND ce.action_activity = tu.action_activity
                                AND ce.video_info_currentTime = tu.video_info_currentTime
                            SET 
                                ce.client_event_timestamp = tu.new_timestamp,
                                ce.client_event_time = FROM_UNIXTIME(tu.new_timestamp + 28800)
                        """)
                    updated_count = cursor.rowcount
                    conn.commit()

                        # 步驟 5: 清理臨時表（可選，因為是 TEMPORARY 表會自動清理）
                        cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_timestamp_updates")
                    
                    total_updated += updated_count
                        print(f"  ✅ 成功更新 {updated_count:,} 筆記錄（臨時表 + JOIN）")
                    
                        log_file.write(
                            f"批次 {batch_num}: 更新 {updated_count:,} 筆記錄（timestamp 範圍: {min_timestamp} ~ {max_timestamp}）\n"
                        )
                    
                except mysql.connector.Error as e:
                    if conn.in_transaction:
                        conn.rollback()
                    # 確保清理臨時表（即使發生錯誤）
                    try:
                        cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_timestamp_updates")
                    except Exception:
                        pass
                    print(f"  ❌ 更新失敗: {e}")
                    log_file.write(f"批次 {batch_num}: 更新失敗 - {e}\n")
                    # 不 raise，繼續處理下一批
                    pass
                except Exception as e:
                    # 確保清理臨時表（即使發生錯誤）
                    try:
                        cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_timestamp_updates")
                    except Exception:
                        pass
                    if conn.in_transaction:
                        conn.rollback()
                    print(f"  ❌ 更新失敗（其他錯誤）: {e}")
                    log_file.write(f"批次 {batch_num}: 更新失敗（其他錯誤） - {e}\n")
                    # 不 raise，繼續處理下一批
                    pass
            
            # 記錄衝突
            if batch_conflicts:
                conflicts_data.extend(batch_conflicts)
                    resolved_count = sum(1 for c in batch_conflicts if c.get('conflict_resolved', False))
                    unresolved_count = len(batch_conflicts) - resolved_count
                print(f"  ⚠️ 發現 {len(batch_conflicts):,} 個衝突組，共 {sum(c['conflict_count'] for c in batch_conflicts):,} 筆記錄")
                    if resolved_count > 0:
                        print(f"    其中 {resolved_count:,} 個衝突組已解決（使用 baseline 時間）")
                    if unresolved_count > 0:
                        print(f"    其中 {unresolved_count:,} 個衝突組無法解決（已跳過）")
                
                # 更新進度表（每批處理完後記錄進度，支援斷點續傳）
                try:
                    # 確保連線正常（進度更新前檢查）
                    conn, cursor = _ensure_connection(conn, cursor, verbose=False)
                    
                    update_progress_query = """
                    INSERT INTO timestamp_fix_progress 
                    (db_name, last_processed_timestamp, last_processed_batch_num, 
                     total_processed, total_updated, total_conflicts)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        last_processed_timestamp = VALUES(last_processed_timestamp),
                        last_processed_batch_num = VALUES(last_processed_batch_num),
                        total_processed = VALUES(total_processed),
                        total_updated = VALUES(total_updated),
                        total_conflicts = VALUES(total_conflicts),
                        updated_at = NOW()
                    """
                    cursor.execute(update_progress_query, (
                        db_name, last_timestamp, batch_num,
                        total_processed, total_updated, total_conflicts
                    ))
                    conn.commit()
                except Exception as e:
                    # 進度更新失敗不影響主流程
                    if conn.in_transaction:
                        conn.rollback()
                    print(f"  ⚠️ 更新進度表失敗（不影響處理）: {e}")
                
                # 範圍查詢不需要 offset，已透過 last_timestamp 更新
        
        # 寫入衝突記錄到 JSON 檔案
        if conflicts_data:
            with open(conflict_log_file, 'w', encoding='utf-8') as conflict_file:
                json.dump({
                    'timestamp': datetime.now().isoformat(),
                        'db_name': db_name,
                    'total_conflicts': len(conflicts_data),
                    'total_conflict_records': sum(c['conflict_count'] for c in conflicts_data),
                    'conflicts': conflicts_data
                }, conflict_file, ensure_ascii=False, indent=2)
            print(f"\n✅ 衝突記錄已寫入：{conflict_log_file}")
        
        # 寫入總結
        log_file.write("\n" + "=" * 80 + "\n")
        log_file.write("修正總結\n")
        log_file.write("=" * 80 + "\n")
        log_file.write(f"總處理記錄數：{total_processed:,}\n")
        log_file.write(f"成功更新記錄數：{total_updated:,}\n")
        log_file.write(f"衝突記錄數：{total_conflicts:,}\n")
        log_file.write(f"衝突組數：{len(conflicts_data):,}\n")
        log_file.write(f"完成時間：{datetime.now()}\n")
    
        # 更新最終進度（標記為完成）
        try:
            final_progress_query = """
            INSERT INTO timestamp_fix_progress 
            (db_name, last_processed_timestamp, last_processed_batch_num, 
             total_processed, total_updated, total_conflicts)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                last_processed_timestamp = VALUES(last_processed_timestamp),
                last_processed_batch_num = VALUES(last_processed_batch_num),
                total_processed = VALUES(total_processed),
                total_updated = VALUES(total_updated),
                total_conflicts = VALUES(total_conflicts),
                updated_at = NOW()
            """
            cursor.execute(final_progress_query, (
                db_name, last_timestamp, batch_num,
                total_processed, total_updated, total_conflicts
            ))
            conn.commit()
            print(f"✅ 進度已更新到進度表")
        except Exception as e:
            if conn.in_transaction:
                conn.rollback()
            print(f"  ⚠️ 更新最終進度失敗: {e}")
    
    cursor.close()
    conn.close()
    
        result = {
            'db_name': db_name,
            'total_processed': total_processed,
            'total_updated': total_updated,
            'total_conflicts': total_conflicts,
            'conflicts_data': conflicts_data,
            'conflict_log_file': conflict_log_file if conflicts_data else None,
            'update_log_file': update_log_file
        }
    
        print(f"\n✅ {db_name} 修正完成！")
        return result
    
except mysql.connector.Error as err:
        print(f"❌ {db_name} 資料庫連線錯誤: {err}")
        return {
            'db_name': db_name,
            'error': str(err),
            'total_processed': 0,
            'total_updated': 0,
            'total_conflicts': 0,
            'conflicts_data': []
        }
except Exception as e:
        print(f"❌ {db_name} 其他錯誤: {e}")
    import traceback
    traceback.print_exc()
        return {
            'db_name': db_name,
            'error': str(e),
            # 保留已跑出的進度，避免看起來「全部歸零」
            'total_processed': total_processed if 'total_processed' in locals() else 0,
            'total_updated': total_updated if 'total_updated' in locals() else 0,
            'total_conflicts': total_conflicts if 'total_conflicts' in locals() else 0,
            'conflicts_data': []
        }

# ==================== 主程式 ====================
# 處理 LAB 資料庫
lab_config = {
    'host': Config.LAB_DB_HOST,
    'port': Config.LAB_DB_PORT,
    'user': Config.LAB_DB_USERNAME,
    'password': Config.LAB_DB_PASSWORD,
    'database': Config.LAB_DB_NAME,
    'charset': 'utf8mb4'
}

# 處理 NCU 資料庫
ncu_config = {
    'host': Config.NCU_DB_HOST,
    'port': Config.NCU_DB_PORT,
    'user': Config.NCU_DB_USERNAME,
    'password': Config.NCU_DB_PASSWORD,
    'database': Config.NCU_DB_NAME,
    'charset': 'utf8mb4'
}

results = []

# 處理 LAB 資料庫
print("\n" + "="*80)
print("開始處理 LAB 資料庫")
print("="*80)
lab_result = fix_database("LAB", lab_config, ServiceLAB)
results.append(lab_result)

# 處理 NCU 資料庫
print("\n" + "="*80)
print("開始處理 NCU 資料庫")
print("="*80)
ncu_result = fix_database("NCU", ncu_config, ServiceNCU)
results.append(ncu_result)

# 輸出總體總結
print("\n" + "="*80)
print("所有資料庫修正完成總結")
print("="*80)
total_processed_all = sum(r.get('total_processed', 0) for r in results)
total_updated_all = sum(r.get('total_updated', 0) for r in results)
total_conflicts_all = sum(r.get('total_conflicts', 0) for r in results)

print(f"總處理記錄數：{total_processed_all:,}")
print(f"總成功更新記錄數：{total_updated_all:,}")
print(f"總衝突記錄數：{total_conflicts_all:,}")

for result in results:
    db_name = result.get('db_name', 'Unknown')
    if 'error' in result:
        print(f"\n{db_name}: ❌ 錯誤 - {result['error']}")
    else:
        print(f"\n{db_name}:")
        print(f"  處理記錄數：{result.get('total_processed', 0):,}")
        print(f"  成功更新記錄數：{result.get('total_updated', 0):,}")
        print(f"  衝突記錄數：{result.get('total_conflicts', 0):,}")
        if result.get('update_log_file'):
            print(f"  更新記錄檔案：{result['update_log_file']}")
        if result.get('conflict_log_file'):
            print(f"  衝突記錄檔案：{result['conflict_log_file']}")

print("="*80)
print(f"完成時間：{datetime.now()}")
print("="*80)
