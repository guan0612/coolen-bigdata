import argparse
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import sys
import os
import urllib.parse

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backend.database.config import Config

STAT_RANGE_START = '2025-08-01'
FILTERS = "action.course_name = 'CoolE Bot'"
LEVEL_MAP = {1: '其他', 2: '高中職', 3: '國中', 4: '國小'}
levels = ['國小', '國中', '高中職', '其他']

# 統計查詢來源：NCU（寫入仍由 ServiceLAB / ServiceNCU 寫入兩邊統計表）
safe_password = urllib.parse.quote_plus(Config.NCU_DB_PASSWORD)
DB_URL = (
    f"mysql+mysqlconnector://{Config.NCU_DB_USERNAME}:{safe_password}"
    f"@{Config.NCU_DB_HOST}:{Config.NCU_DB_PORT}/{Config.NCU_DB_NAME}"
)
engine = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=3600)


def get_raw_query(period_col, date_format, start_str, end_str):
    return f"""
    SELECT 
        DATE_FORMAT(ce.client_event_time, '{date_format}') AS {period_col},
        actor.uid AS uid,
        MIN(CASE 
            WHEN actor.category = '國小' THEN 4
            WHEN actor.category = '國中' THEN 3
            WHEN actor.category IN ('普高', '技高') THEN 2
            ELSE 1
        END) AS priority,
        COUNT(*) AS instances,
        SUM(CASE WHEN action.activity = 'aichatbot_SendMessage' THEN 1 ELSE 0 END) AS dialogs
    FROM client_event ce
    JOIN actor ON ce.actor_id = actor.id
    JOIN action ON ce.action_id = action.id
    WHERE ce.client_event_time >= '{start_str}'
      AND ce.client_event_time < '{end_str}'
      AND {FILTERS}
      AND actor.uid != '1'
    GROUP BY {period_col}, actor.uid
    """

def build_pivots(df_raw, period_col):
    uid_meta = df_raw.groupby([period_col, 'uid'])['priority'].min().reset_index()
    uid_meta['student_level'] = uid_meta['priority'].map(LEVEL_MAP)
    df_merged = pd.merge(df_raw, uid_meta[[period_col, 'uid', 'student_level']], on=[period_col, 'uid'])

    pivot_instances = df_merged.pivot_table(index=period_col, columns='student_level', values='instances', aggfunc='sum', fill_value=0)
    pivot_users = df_merged.pivot_table(index=period_col, columns='student_level', values='uid', aggfunc='nunique', fill_value=0)
    pivot_dialogs = df_merged.pivot_table(index=period_col, columns='student_level', values='dialogs', aggfunc='sum', fill_value=0)

    for p in [pivot_instances, pivot_users, pivot_dialogs]:
        for l in levels:
            if l not in p.columns:
                p[l] = 0

    pivot_instances = pivot_instances[levels].sort_index()
    pivot_users = pivot_users[levels].sort_index()
    pivot_dialogs = pivot_dialogs.reindex(pivot_instances.index, fill_value=0)[levels]

    pivot_instances['總計'] = pivot_instances.sum(axis=1)
    pivot_users['總計'] = pivot_users.sum(axis=1)
    pivot_dialogs['總計'] = pivot_dialogs.sum(axis=1)

    avg_total = (pivot_dialogs['總計'] / pivot_users['總計'].replace(0, pd.NA)).fillna(0).round(2)
    return pivot_instances, pivot_users, pivot_dialogs, avg_total


def run_daily():
    """每日統計：僅統計該日當天的數據（不含累計）"""
    today = datetime.now().date()
    # 統計最近三天，確保若有延遲也能補上數據
    start_dt = today - timedelta(days=30)
    end_dt = today + timedelta(days=1)

    all_dfs = []
    current = start_dt
    while current < end_dt:
        stat_date_str = current.strftime('%Y-%m-%d')
        # 重點：將開始與結束時間都設在同一天
        batch_start = stat_date_str
        batch_end = (current + timedelta(days=1)).strftime('%Y-%m-%d')
        
        try:
            with engine.connect() as conn:
                conn.execute(text("SET SESSION net_read_timeout = 3600"))
                # 直接調用 get_raw_query 並傳入 '%Y-%m-%d'，確保依日期分群
                query = get_raw_query('stat_date', '%Y-%m-%d', batch_start, batch_end)
                df = pd.read_sql_query(text(query), conn)
            if not df.empty:
                all_dfs.append(df)
        except Exception as e:
            print(f"Error on {stat_date_str}: {e}")
            pass
        current = current + timedelta(days=1)

    if not all_dfs:
        return

    df_raw = pd.concat(all_dfs).reset_index(drop=True)
    # 統一使用 'stat_date' 作為 pivot 的 index
    pivot_instances, pivot_users, pivot_dialogs, avg_total = build_pivots(df_raw, 'stat_date')

    batch_date = datetime.now().date()
    stats_data = []
    for stat_date in pivot_instances.index:
        for level in levels + ['總計']:
            avg_t = float(avg_total.loc[stat_date]) if level == '總計' else 0.0
            stats_data.append({
                'stat_date': stat_date if isinstance(stat_date, str) else stat_date.strftime('%Y-%m-%d'),
                'student_level': level,
                'usage_instances': int(pivot_instances.loc[stat_date, level]),
                'usage_users': int(pivot_users.loc[stat_date, level]),
                'cumulative_users': 0,
                'avg_dialog_turns': avg_t
            })

    from backend.database.service_lab import ServiceLAB
    from backend.database.service_ncu import ServiceNCU
    for svc_cls in [ServiceLAB, ServiceNCU]:
        try:
            db = svc_cls()
            db.createTable_coolebot_stats_daily()
            db.insert_coolebot_stats_daily(stats_data, batch_date)
            db.db.close()
        except Exception:
            pass


def run_monthly():
    """月統計優化版：按天分批抓取數據，解決 MySQL 2013 斷線問題。"""
    today = datetime.now().date()
    start_dt = datetime.strptime(STAT_RANGE_START, '%Y-%m-%d').date()
    # 統計到下個月 1 號為止
    end_dt = (today + timedelta(days=32)).replace(day=1)

    all_dfs = []
    current_day = start_dt
    
    print(f"--- 每月統計啟動 (優化模式：按天抓取) ---")

    while current_day < end_dt and current_day <= today:
        day_start = current_day.strftime('%Y-%m-%d')
        day_end = (current_day + timedelta(days=1)).strftime('%Y-%m-%d')
        month_label = current_day.strftime('%Y-%m')
        
        try:
            with engine.connect() as conn:
                conn.execute(text("SET SESSION net_read_timeout = 3600"))
                # 這裡調用 get_raw_query，並指定 period_col 為 'month' 且 format 為 '%Y-%m'
                # 這樣抓出來的每一天資料，都會被打上所屬「月份」的標籤
                query_text = get_raw_query('month', '%Y-%m', day_start, day_end)
                df_day = pd.read_sql_query(text(query_text), conn)
                
            if not df_day.empty:
                all_dfs.append(df_day)
            
            # 每週印一次進度
            if current_day.day % 7 == 0 or current_day == today:
                print(f"  已完成抓取至: {day_start}")
                
        except Exception as e:
            print(f"  Error on {day_start}: {e}")
            
        current_day += timedelta(days=1)

    if not all_dfs:
        print("❌ 未能取得任何資料。")
        return

    # --- 後續邏輯完全銜接原本的 Pandas 處理 ---
    print("正在彙整數據並計算累計人數...")
    df_raw = pd.concat(all_dfs).reset_index(drop=True)
    
    # 這裡傳入 'month'，因為我們上面的 SQL 已經把 period 標為月份了
    pivot_instances, pivot_users, pivot_dialogs, avg_total = build_pivots(df_raw, 'month')

    # 計算累計人數 (開站至今)
    first_seen = df_raw.groupby('uid')['month'].min().reset_index()
    cumulative_dict = {}
    for m in sorted(pivot_users.index):
        cumulative_dict[m] = first_seen[first_seen['month'] <= m]['uid'].nunique()
    pivot_users['累計'] = pd.Series(cumulative_dict)

    batch_date = datetime.now().date()
    stats_data = []
    for month in pivot_instances.index:
        for level in levels + ['總計']:
            cum = int(pivot_users.loc[month, '累計']) if level == '總計' else 0
            avg_t = float(avg_total.loc[month]) if level == '總計' else 0.0
            stats_data.append({
                'month': month,
                'student_level': level,
                'usage_instances': int(pivot_instances.loc[month, level]),
                'usage_users': int(pivot_users.loc[month, level]),
                'cumulative_users': cum,
                'avg_dialog_turns': avg_t
            })

    from backend.database.service_lab import ServiceLAB
    from backend.database.service_ncu import ServiceNCU
    for svc_cls in [ServiceLAB, ServiceNCU]:
        try:
            db = svc_cls()
            db.createTable_coolebot_stats()
            db.insert_coolebot_stats(stats_data, batch_date)
            db.db.close()
            print(f"✅ 成功寫入 {svc_cls.__name__} 資料庫")
        except Exception as e:
            print(f"❌ 寫入失敗: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['daily', 'monthly'], default='daily')
    args = parser.parse_args()
    if args.mode == 'daily':
        run_daily()
    else:
        run_monthly()


if __name__ == '__main__':
    main()
