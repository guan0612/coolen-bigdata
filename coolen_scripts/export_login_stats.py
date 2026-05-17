import argparse
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import sys
import os
import urllib.parse
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backend.database.config import Config

safe_password = urllib.parse.quote_plus(Config.NCU_DB_PASSWORD)
DB_URL = (
    f"mysql+mysqlconnector://{Config.NCU_DB_USERNAME}:{safe_password}"
    f"@{Config.NCU_DB_HOST}:{Config.NCU_DB_PORT}/{Config.NCU_DB_NAME}"
)
engine = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=3600)

all_levels = ['國小', '國中', '高中職', '其他']
level_map = {1: '其他', 2: '高中職', 3: '國中', 4: '國小'}
STAT_RANGE_START = '2025-08-01'

def get_login_query(date_format, period_alias, start_str, end_str):
    """
    還原原本的明細抓取邏輯 (UID, Session)。
    """
    start_unix = int(time.mktime(time.strptime(start_str, "%Y-%m-%d"))) - 28800
    end_unix = int(time.mktime(time.strptime(end_str, "%Y-%m-%d"))) - 28800

    period_expr = f"DATE_FORMAT(FROM_UNIXTIME(ce.server_token_unix + 28800), '{date_format}')"

    return f"""
    SELECT 
        raw.{period_alias},
        raw.uid,
        raw.session,
        raw.priority
    FROM (
        SELECT 
            {period_expr} AS {period_alias},
            actor.uid AS uid,
            actor.session AS session,
            MIN(CASE 
                WHEN actor.category = '國小' THEN 4
                WHEN actor.category = '國中' THEN 3
                WHEN actor.category IN ('普高', '技高') THEN 2
                ELSE 1
            END) AS priority
        FROM client_event AS ce FORCE INDEX (idx_token_unix)
        JOIN actor ON ce.actor_id = actor.id
        WHERE ce.server_token_unix >= {start_unix}
          AND ce.server_token_unix < {end_unix}
          AND actor.session IS NOT NULL
          AND actor.session != ''
          AND actor.uid != '1'
        GROUP BY {period_alias}, actor.uid, actor.session 
    ) AS raw
    """

def _build_pivots(df_raw):
    """還原原本的 Pandas 樞紐分析邏輯。"""
    if df_raw.empty:
        return pd.DataFrame(), pd.DataFrame()
        
    uid_period_main = df_raw.groupby(['period', 'uid'])['priority'].min().reset_index()
    uid_period_main['student_level'] = uid_period_main['priority'].map(level_map)
    df_merged = pd.merge(df_raw, uid_period_main[['period', 'uid', 'student_level']], on=['period', 'uid'])

    pivot_instances = df_merged.pivot_table(index='period', columns='student_level', values='session', aggfunc='count', fill_value=0)
    pivot_users = df_merged.pivot_table(index='period', columns='student_level', values='uid', aggfunc='nunique', fill_value=0)

    for level in all_levels:
        if level not in pivot_instances.columns: pivot_instances[level] = 0
        if level not in pivot_users.columns: pivot_users[level] = 0

    pivot_instances = pivot_instances[all_levels].sort_index()
    pivot_users = pivot_users[all_levels].sort_index()
    
    pivot_instances['總計'] = pivot_instances.sum(axis=1)
    pivot_users['總計'] = pivot_users.sum(axis=1)

    return pivot_instances, pivot_users

def run_daily():
    """每日統計：修正為『當天』統計，避免數據堆疊錯誤。"""
    today = datetime.now().date()
    start_dt = today - timedelta(days=30)
    end_dt = today + timedelta(days=1)

    all_dfs = []
    current = start_dt
    while current < end_dt:
        stat_date_str = current.strftime('%Y-%m-%d')
        batch_end = (current + timedelta(days=1)).strftime('%Y-%m-%d')
        try:
            with engine.connect() as conn:
                conn.execute(text("SET SESSION net_read_timeout = 36000"))
                conn.execute(text("SET SESSION net_write_timeout = 36000"))
                # 修改這裡：傳入 stat_date_str 作為開始，確保只抓一天
                query = get_login_query('%Y-%m-%d', 'period', stat_date_str, batch_end)
                df = pd.read_sql_query(text(query), conn)
                if not df.empty:
                    all_dfs.append(df)
        except Exception as e:
            print(f"Error on {stat_date_str}: {e}")
        current += timedelta(days=1)

    if not all_dfs:
        return
    df_raw = pd.concat(all_dfs).reset_index(drop=True)
    df_raw = df_raw.rename(columns={'period': 'period'})
    pivot_instances, pivot_users = _build_pivots(df_raw)
    _save_to_db(pivot_instances, pivot_users, mode='daily')

def run_monthly():
    """月統計優化版：將原本「按月查詢」改為「按天查詢」後彙整。"""
    today = datetime.now().date()
    range_start = datetime.strptime(STAT_RANGE_START, '%Y-%m-%d').date().replace(day=1)
    start_dt = max((today - timedelta(days=365)).replace(day=1), range_start)
    end_dt = (today + timedelta(days=32)).replace(day=1)

    all_dfs = []
    current_month = start_dt
    
    while current_month < end_dt:
        next_month = (current_month + timedelta(days=32)).replace(day=1)
        print(f"--- 處理月份: {current_month.strftime('%Y-%m')} ---")
        
        # --- 重點優化：在月份內拆成每一天抓取 ---
        day_cursor = current_month
        while day_cursor < next_month and day_cursor <= today:
            day_start_str = day_cursor.strftime('%Y-%m-%d')
            day_end_str = (day_cursor + timedelta(days=1)).strftime('%Y-%m-%d')
            
            try:
                with engine.connect() as conn:
                    # 提高超時設定（預防萬一）
                    conn.execute(text("SET SESSION net_read_timeout = 36000"))
                    # 依然使用 get_login_query，但時間範圍縮小到「一天」
                    query = get_login_query('%Y-%m', 'period', day_start_str, day_end_str)
                    df = pd.read_sql_query(text(query), conn)
                    if not df.empty:
                        all_dfs.append(df)
            except Exception as e:
                print(f"  Error fetching day {day_start_str}: {e}")
            
            day_cursor += timedelta(days=1)
        # ----------------------------------------
        
        current_month = next_month

    # 以下邏輯完全維持你原本的正確寫法
    if not all_dfs:
        print("未抓取到任何資料")
        return
        
    df_raw = pd.concat(all_dfs).reset_index(drop=True)
    pivot_instances, pivot_users = _build_pivots(df_raw)

    cutoff_month = STAT_RANGE_START[:7]
    valid_periods = [p for p in pivot_instances.index if p >= cutoff_month]
    if not valid_periods: return
    
    pivot_instances = pivot_instances.loc[valid_periods]
    pivot_users = pivot_users.loc[valid_periods]
    df_raw = df_raw[df_raw['period'].isin(valid_periods)]

    first_seen = df_raw.groupby('uid')['period'].min().reset_index()
    cumulative_dict = {}
    for p in sorted(pivot_users.index):
        count = first_seen[first_seen['period'] <= p]['uid'].nunique()
        cumulative_dict[p] = count
        
    pivot_users['累計'] = pd.Series(cumulative_dict)
    _save_to_db(pivot_instances, pivot_users, mode='monthly')
    print("✅ 月統計處理完成並存入資料庫")

def _save_to_db(pivot_instances, pivot_users, mode):
    from backend.database.service_lab import ServiceLAB
    from backend.database.service_ncu import ServiceNCU

    batch_date = datetime.now().date()
    stats_data = []

    for period in pivot_instances.index:
        for level in all_levels + ['總計']:
            cumulative = int(pivot_users.loc[period, '累計']) if (mode == 'monthly' and level == '總計') else 0
            row = {
                'student_level': level,
                'login_instances': int(pivot_instances.loc[period, level]),
                'login_users': int(pivot_users.loc[period, level]),
                'cumulative_users': cumulative
            }
            if mode == 'daily': row['stat_date'] = period
            else: row['month'] = period
            stats_data.append(row)

    for svc_cls in [ServiceLAB, ServiceNCU]:
        try:
            db = svc_cls()
            if mode == 'daily':
                db.createTable_login_stats_daily()
                db.insert_login_stats_daily(stats_data, batch_date)
            else:
                db.createTable_login_stats()
                db.insert_login_stats(stats_data, batch_date)
            db.db.close()
        except: pass

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['daily', 'monthly'], default='daily')
    args = parser.parse_args()
    if args.mode == 'daily': run_daily()
    else: run_monthly()

if __name__ == '__main__':
    main()