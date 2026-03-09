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

safe_password = urllib.parse.quote_plus(Config.LAB_DB_PASSWORD)
DB_URL = f"mysql+mysqlconnector://{Config.LAB_DB_USERNAME}:{safe_password}@{Config.LAB_DB_HOST}:{Config.LAB_DB_PORT}/{Config.LAB_DB_NAME}"
engine = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=3600)


def get_raw_query(period_col, date_format, start_str, end_str):
    return f"""
    SELECT 
        DATE_FORMAT(ce.client_event_time, '{date_format}') AS {period_col},
        actor.uid AS uid,
        action.activity,
        CASE 
            WHEN actor.category = '國小' THEN 4
            WHEN actor.category = '國中' THEN 3
            WHEN actor.category IN ('普高', '技高') THEN 2
            ELSE 1
        END AS priority
    FROM client_event ce
    JOIN actor ON ce.actor_id = actor.id
    JOIN action ON ce.action_id = action.id
    WHERE ce.client_event_time >= '{start_str}'
      AND ce.client_event_time < '{end_str}'
      AND {FILTERS}
    """


def build_pivots(df_raw, period_col):
    uid_meta = df_raw.groupby([period_col, 'uid'])['priority'].min().reset_index()
    uid_meta['student_level'] = uid_meta['priority'].map(LEVEL_MAP)
    df_merged = pd.merge(df_raw, uid_meta[[period_col, 'uid', 'student_level']], on=[period_col, 'uid'])
    df_merged['is_dialog'] = df_merged['activity'] == 'aichatbot_SendMessage'

    pivot_instances = df_merged.pivot_table(index=period_col, columns='student_level', values='priority', aggfunc='count', fill_value=0)
    pivot_users = df_merged.pivot_table(index=period_col, columns='student_level', values='uid', aggfunc='nunique', fill_value=0)
    pivot_dialogs = df_merged[df_merged['is_dialog']].pivot_table(index=period_col, columns='student_level', values='priority', aggfunc='count', fill_value=0)

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
    today = datetime.now().date()
    start_date = max((today - timedelta(days=3)).strftime('%Y-%m-%d'), STAT_RANGE_START)
    end_dt = (today + timedelta(days=1))

    all_dfs = []
    current = datetime.strptime(start_date, '%Y-%m-%d').date()
    while current < end_dt:
        batch_end = (current + timedelta(days=1)).strftime('%Y-%m-%d')
        batch_start = current.strftime('%Y-%m-%d')
        try:
            with engine.connect() as conn:
                conn.execute(text("SET SESSION net_read_timeout = 3600"))
                df = pd.read_sql_query(text(get_raw_query('stat_date', '%Y-%m-%d', batch_start, batch_end)), conn)
            if not df.empty:
                all_dfs.append(df)
        except Exception:
            pass
        current = current + timedelta(days=1)

    if not all_dfs:
        return

    df_raw = pd.concat(all_dfs).reset_index(drop=True)
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
    today = datetime.now().date()
    start_dt = datetime.strptime(STAT_RANGE_START, '%Y-%m-%d').date()
    end_dt = (today + timedelta(days=32)).replace(day=1)

    all_dfs = []
    current = start_dt
    while current < end_dt:
        batch_start = current.strftime('%Y-%m-%d')
        if current.month == 12:
            next_m = current.replace(year=current.year + 1, month=1)
        else:
            next_m = current.replace(month=current.month + 1)
        batch_end = next_m.strftime('%Y-%m-%d')
        try:
            with engine.connect() as conn:
                conn.execute(text("SET SESSION net_read_timeout = 3600"))
                df = pd.read_sql_query(text(get_raw_query('month', '%Y-%m', batch_start, batch_end)), conn)
            if not df.empty:
                all_dfs.append(df)
        except Exception:
            pass
        current = next_m

    if not all_dfs:
        return

    df_raw = pd.concat(all_dfs).reset_index(drop=True)
    pivot_instances, pivot_users, pivot_dialogs, avg_total = build_pivots(df_raw, 'month')

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
        except Exception:
            pass


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
