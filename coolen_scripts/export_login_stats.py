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

safe_password = urllib.parse.quote_plus(Config.LAB_DB_PASSWORD)
DB_URL = f"mysql+mysqlconnector://{Config.LAB_DB_USERNAME}:{safe_password}@{Config.LAB_DB_HOST}:{Config.LAB_DB_PORT}/{Config.LAB_DB_NAME}"
engine = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=3600)

all_levels = ['國小', '國中', '高中職', '其他']
level_map = {1: '其他', 2: '高中職', 3: '國中', 4: '國小'}

def get_login_query(date_format, period_alias, start_str, end_str):
    """
    實裝 FORCE INDEX 與虛擬列優化版。
    """
    # 先將日期轉為 Unix Timestamp (需考慮 +8 時區補償)
    import time
    start_unix = int(time.mktime(time.strptime(start_str, "%Y-%m-%d"))) - 28800
    end_unix = int(time.mktime(time.strptime(end_str, "%Y-%m-%d"))) - 28800

    return f"""
    SELECT 
        raw.{period_alias},
        raw.uid,
        raw.session,
        raw.priority
    FROM (
        SELECT 
            DATE_FORMAT(FROM_UNIXTIME(ce.server_token_unix + 28800), '{date_format}') AS {period_alias},
            actor.uid AS uid,
            actor.session AS session,
            MIN(CASE 
                WHEN actor.category = '國小' THEN 4
                WHEN actor.category = '國中' THEN 3
                WHEN actor.category IN ('普高', '技高') THEN 2
                ELSE 1
            END) AS priority
        FROM client_event AS ce FORCE INDEX (idx_token_unix) -- 強制走索引
        JOIN actor ON ce.actor_id = actor.id
        WHERE ce.server_token_unix >= {start_unix}
          AND ce.server_token_unix < {end_unix}
          AND actor.session IS NOT NULL
          AND actor.session != ''
        GROUP BY actor.uid, actor.session
    ) AS raw
    """

def _build_pivots(df_raw):
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
    today = datetime.now().date()
    start_dt = today - timedelta(days=3)
    end_dt = today + timedelta(days=1)

    all_dfs = []
    current = start_dt
    while current < end_dt:
        batch_start = current.strftime('%Y-%m-%d')
        batch_end = (current + timedelta(days=1)).strftime('%Y-%m-%d')
        try:
            with engine.connect() as conn:
                conn.execute(text("SET SESSION net_read_timeout = 3600"))
                query = get_login_query('%Y-%m-%d', 'stat_date', batch_start, batch_end)
                df = pd.read_sql_query(text(query), conn)
                if not df.empty:
                    all_dfs.append(df)
        except Exception:
            pass
        current += timedelta(days=1)

    if not all_dfs: return
    df_raw = pd.concat(all_dfs).reset_index(drop=True)
    df_raw = df_raw.rename(columns={'stat_date': 'period'})
    pivot_instances, pivot_users = _build_pivots(df_raw)
    _save_to_db(pivot_instances, pivot_users, mode='daily')

def run_monthly():
    today = datetime.now().date()
    start_dt = (today - timedelta(days=365)).replace(day=1)
    end_dt = (today + timedelta(days=32)).replace(day=1)

    all_dfs = []
    current = start_dt
    while current < end_dt:
        batch_start = current.strftime('%Y-%m-%d')
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1)
        else:
            next_month = current.replace(month=current.month + 1)
        batch_end = next_month.strftime('%Y-%m-%d')
        
        try:
            with engine.connect() as conn:
                conn.execute(text("SET SESSION net_read_timeout = 3600"))
                query = get_login_query('%Y-%m', 'month', batch_start, batch_end)
                df = pd.read_sql_query(text(query), conn)
                if not df.empty:
                    all_dfs.append(df)
        except Exception:
            pass
        current = next_month

    if not all_dfs: return
    df_raw = pd.concat(all_dfs).reset_index(drop=True)
    df_raw = df_raw.rename(columns={'month': 'period'})
    pivot_instances, pivot_users = _build_pivots(df_raw)

    first_seen = df_raw.groupby('uid')['period'].min().reset_index()
    cumulative_dict = {}
    for p in sorted(pivot_users.index):
        count = first_seen[first_seen['period'] <= p]['uid'].nunique()
        cumulative_dict[p] = count
    pivot_users['累計'] = pd.Series(cumulative_dict)
    _save_to_db(pivot_instances, pivot_users, mode='monthly')

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
            if mode == 'daily':
                row['stat_date'] = period
            else:
                row['month'] = period
            stats_data.append(row)

    for svc_cls, name in [(ServiceLAB, 'LAB'), (ServiceNCU, 'NCU')]:
        try:
            db = svc_cls()
            if mode == 'daily':
                db.createTable_login_stats_daily()
                db.insert_login_stats_daily(stats_data, batch_date)
            else:
                db.createTable_login_stats()
                db.insert_login_stats(stats_data, batch_date)
            db.db.close()
        except Exception:
            pass

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['daily', 'monthly'], default='daily')
    args = parser.parse_args()
    if args.mode == 'daily': run_daily()
    else: run_monthly()

if __name__ == '__main__':
    main()