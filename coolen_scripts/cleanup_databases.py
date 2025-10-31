#!/usr/bin/env python3
"""
同時清理 LAB 和 NCU 資料庫
保留最近 3 個月的資料
"""

import sys
sys.path.insert(0, '/app')

from datetime import datetime, timedelta
from backend.database.service_lab import ServiceLAB
from backend.database.service_ncu import ServiceNCU

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

def cleanup_database(db_service, db_name, cutoff_date):
    """清理資料庫"""
    log(f"{'='*60}")
    log(f"開始清理 {db_name} 資料庫")
    log(f"{'='*60}")
    log(f"清理目標：{cutoff_date} 之前的資料")
    
    tables = ['client_event', 'actor', 'action', 'result', 
              'event_info', 'video_info', 'cookie', 'tanet_info', 'sentences']
    
    total_deleted = 0
    
    try:
        for table in tables:
            log(f"[{db_name}] 清理表 {table}...")
            table_deleted = 0
            batch_size = 50000
            
            while True:
                try:
                    # 正確的 SQL：提取檔名中的日期並比較
                    sql = f"""
                        DELETE FROM {table} 
                        WHERE (
                            -- 格式1: 20250423-17.json (YYYYMMDD)
                            (filename REGEXP '^[0-9]{{8}}-' 
                             AND STR_TO_DATE(SUBSTRING(filename, 1, 8), '%Y%m%d') < '{cutoff_date}')
                            OR
                            -- 格式2: 2025-08-01_1-4794.json (YYYY-MM-DD)
                            (filename REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}_'
                             AND STR_TO_DATE(SUBSTRING(filename, 1, 10), '%Y-%m-%d') < '{cutoff_date}')
                        )
                        LIMIT {batch_size}
                    """
                    
                    db_service.cursor.execute(sql)
                    affected = db_service.cursor.rowcount
                    db_service.db.commit()
                    
                    if affected == 0:
                        break
                    
                    table_deleted += affected
                    total_deleted += affected
                    
                    if table_deleted % 500000 == 0:
                        log(f"  [{db_name}] {table}: 已刪除 {table_deleted:,} 筆...")
                    
                except Exception as e:
                    log(f"  ✗ [{db_name}] {table} 刪除錯誤: {e}")
                    db_service.db.rollback()
                    break
            
            if table_deleted > 0:
                log(f"  ✓ [{db_name}] {table}: 刪除 {table_deleted:,} 筆")
            else:
                log(f"  ✓ [{db_name}] {table}: 無需清理")
        
        log(f"[{db_name}] 清理完成！共刪除 {total_deleted:,} 筆資料")
        return total_deleted
        
    except Exception as e:
        log(f"✗ [{db_name}] 清理失敗: {e}")
        return 0
    finally:
        try:
            db_service.cursor.close()
            db_service.db.close()
        except:
            pass

def main():
    log("="*60)
    log("資料庫清理程式啟動")
    log("="*60)
    
    # 計算 3 個月前的日期（改為完整日期格式）
    cutoff = datetime.now() - timedelta(days=90)
    cutoff_date = cutoff.strftime('%Y-%m-%d')  # 改為 YYYY-MM-DD
    
    log(f"保留：{cutoff_date} 之後的資料")
    log(f"刪除：{cutoff_date} 及更早的資料")
    
    # 清理 LAB
    try:
        log("連接 LAB 資料庫...")
        db_lab = ServiceLAB()
        lab_deleted = cleanup_database(db_lab, "LAB", cutoff_date)
    except Exception as e:
        log(f"LAB 清理失敗: {e}")
        lab_deleted = 0
    
    # 清理 NCU
    try:
        log("連接 NCU 資料庫...")
        db_ncu = ServiceNCU()
        ncu_deleted = cleanup_database(db_ncu, "NCU", cutoff_date)
    except Exception as e:
        log(f"NCU 清理失敗: {e}")
        ncu_deleted = 0
    
    log("="*60)
    log(f"總計刪除：LAB {lab_deleted:,} 筆 + NCU {ncu_deleted:,} 筆")
    log("清理程式完成！")
    log("="*60)

if __name__ == "__main__":
    main()

