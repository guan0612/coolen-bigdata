"""
使用統計：支援 daily / monthly 兩種模式。
統計範圍固定為 2025-08-01 至目前，避免錯誤的 client_event_time 影響累計。

  --mode daily  : 寫入 usage_stats_daily（近 3 天，不含累計）
  --mode monthly: 寫入 usage_stats（每月人次、人數、累計）
"""
import argparse
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backend.database.config import Config

# 統計範圍起點（避免錯誤資料影響累計）
STAT_RANGE_START = '2025-08-01'

DB_URL = f"mysql+mysqlconnector://{Config.LAB_DB_USERNAME}:{Config.LAB_DB_PASSWORD}@{Config.LAB_DB_HOST}:{Config.LAB_DB_PORT}/{Config.LAB_DB_NAME}"
engine = create_engine(DB_URL)

levels = ['國小', '國中', '高中職', '其他']


def get_daily_query(start_date, end_date):
    """依日彙總的查詢（範圍限制在 STAT_RANGE_START 之後）"""
    start = max(start_date, STAT_RANGE_START)
    return f"""
    SELECT 
        sub.day_label AS stat_date,
        CASE 
            WHEN sub.min_priority = 1 THEN '其他'
            WHEN sub.min_priority = 2 THEN '高中職'
            WHEN sub.min_priority = 3 THEN '國中'
            ELSE '國小'
        END AS student_level,
        SUM(sub.usage_instances) AS usage_instances,
        COUNT(DISTINCT sub.uid) AS usage_users
    FROM (
        SELECT 
            DATE_FORMAT(ce.client_event_time, '%Y-%m-%d') AS day_label,
            a.uid,
            MIN(CASE 
                WHEN a.category = '國小' THEN 4
                WHEN a.category = '國中' THEN 3
                WHEN a.category IN ('普高', '技高') THEN 2
                ELSE 1
            END) AS min_priority,
            COUNT(*) AS usage_instances
        FROM client_event AS ce
        JOIN actor AS a ON ce.actor_id = a.id
        WHERE ce.client_event_time >= '{start}' 
          AND ce.client_event_time < '{end_date}'
        GROUP BY day_label, a.uid
    ) AS sub
    GROUP BY stat_date, student_level
    """


def get_monthly_query(start_date, end_date):
    """依月彙總的查詢"""
    return f"""
    SELECT 
        sub.month_label AS month,
        CASE 
            WHEN sub.min_priority = 1 THEN '其他'
            WHEN sub.min_priority = 2 THEN '高中職'
            WHEN sub.min_priority = 3 THEN '國中'
            ELSE '國小'
        END AS student_level,
        SUM(sub.usage_instances) AS usage_instances,
        COUNT(DISTINCT sub.uid) AS usage_users
    FROM (
        SELECT 
            DATE_FORMAT(ce.client_event_time, '%Y-%m') AS month_label,
            a.uid,
            MIN(CASE 
                WHEN a.category = '國小' THEN 4
                WHEN a.category = '國中' THEN 3
                WHEN a.category IN ('普高', '技高') THEN 2
                ELSE 1
            END) AS min_priority,
            COUNT(*) AS usage_instances
        FROM client_event AS ce
        JOIN actor AS a ON ce.actor_id = a.id
        WHERE ce.client_event_time >= '{start_date}' 
          AND ce.client_event_time < '{end_date}'
        GROUP BY month_label, a.uid
    ) AS sub
    GROUP BY month, student_level
    """


def get_monthly_uids_query(month_start, month_end):
    """查詢單月內的不重複 uid（用於累積人數計算）"""
    return f"""
    SELECT DISTINCT a.uid
    FROM client_event AS ce
    JOIN actor AS a ON ce.actor_id = a.id
    WHERE ce.client_event_time >= '{month_start}'
      AND ce.client_event_time < '{month_end}'
    """


def run_daily():
    """每日統計：近 3 天，寫入 usage_stats_daily，cumulative=0"""
    today = datetime.now().date()
    start_date = max(
        (today - timedelta(days=3)).strftime('%Y-%m-%d'),
        STAT_RANGE_START
    )
    end_date = (today + timedelta(days=1)).strftime('%Y-%m-%d')

    print(f"每日統計（範圍 {STAT_RANGE_START} ~ 目前）")
    print(f"處理日期：{start_date} ~ {today}...")

    all_dfs = []
    current = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()

    while current < end_dt:
        batch_end = min(current + timedelta(days=30), end_dt)
        batch_start_str = current.strftime('%Y-%m-%d')
        batch_end_str = batch_end.strftime('%Y-%m-%d')
        print(f"  處理 {batch_start_str} ~ {batch_end_str}...")
        try:
            with engine.connect() as conn:
                df = pd.read_sql_query(text(get_daily_query(batch_start_str, batch_end_str)), conn)
            if not df.empty:
                all_dfs.append(df)
        except Exception as e:
            print(f"  查詢失敗: {e}")
        current = batch_end

    if not all_dfs:
        print("未能取得任何資料。")
        return

    final_df = pd.concat(all_dfs).reset_index(drop=True)
    final_df['student_level'] = pd.Categorical(final_df['student_level'], categories=levels, ordered=True)
    final_df = final_df.sort_values(['stat_date', 'student_level'])

    pivot_instances = final_df.pivot_table(index='stat_date', columns='student_level', values='usage_instances', fill_value=0, aggfunc='sum')
    pivot_users = final_df.pivot_table(index='stat_date', columns='student_level', values='usage_users', fill_value=0, aggfunc='sum')

    for level in levels:
        if level not in pivot_instances.columns:
            pivot_instances[level] = 0
        if level not in pivot_users.columns:
            pivot_users[level] = 0

    pivot_instances = pivot_instances[levels].sort_index()
    pivot_users = pivot_users[levels].sort_index()
    pivot_instances['總計'] = pivot_instances.sum(axis=1)
    pivot_users['總計'] = pivot_users[levels].sum(axis=1)

    batch_date = datetime.now().date()
    stats_data = []
    for stat_date in pivot_instances.index:
        for level in levels + ['總計']:
            stats_data.append({
                'stat_date': stat_date if isinstance(stat_date, str) else stat_date.strftime('%Y-%m-%d'),
                'student_level': level,
                'usage_instances': int(pivot_instances.loc[stat_date, level]),
                'usage_users': int(pivot_users.loc[stat_date, level]),
                'cumulative_users': 0
            })

    from backend.database.service_lab import ServiceLAB
    from backend.database.service_ncu import ServiceNCU

    for name, svc_cls in [('LAB', ServiceLAB), ('NCU', ServiceNCU)]:
        try:
            db = svc_cls()
            db.createTable_usage_stats_daily()
            n = db.insert_usage_stats_daily(stats_data, batch_date)
            db.cursor.close()
            db.db.close()
            print(f"成功寫入 {n} 筆每日統計到 {name} 資料庫")
        except Exception as e:
            print(f"寫入 {name} 失敗: {e}")
            import traceback
            traceback.print_exc()


def run_monthly():
    """每月統計：2025-08 至目前，寫入 usage_stats（含累計）。
    改為按月分批查詢，避免單次大查詢逾時。累積人數以每月 uid 集合聯集計算。"""
    today = datetime.now().date()
    end_dt = (today + timedelta(days=32)).replace(day=1)
    start_dt = datetime.strptime(STAT_RANGE_START, '%Y-%m-%d').date()

    print(f"每月統計（範圍 {STAT_RANGE_START} ~ 目前）")

    all_dfs = []
    cumulative_set = set()
    cumulative_map = {}
    current = start_dt

    while current < end_dt:
        y, m = current.year, current.month
        month_label = f"{y}-{m:02d}"
        if m == 12:
            next_month = f"{y + 1}-01-01"
        else:
            next_month = f"{y}-{m + 1:02d}-01"
        month_start = current.strftime('%Y-%m-%d')

        print(f"  處理月份 {month_label}...")
        try:
            with engine.connect() as conn:
                df = pd.read_sql_query(text(get_monthly_query(month_start, next_month)), conn)
                df_uids = pd.read_sql_query(text(get_monthly_uids_query(month_start, next_month)), conn)
            if not df.empty:
                all_dfs.append(df)
            month_uids = set(df_uids['uid'].dropna().astype(str))
            cumulative_set |= month_uids
            cumulative_map[month_label] = len(cumulative_set)
        except Exception as e:
            print(f"    查詢失敗: {e}")
            prev = f"{y}-{m-1:02d}" if m > 1 else f"{y-1}-12"
            cumulative_map[month_label] = cumulative_map.get(prev, 0)

        current = (current.replace(day=28) + timedelta(days=4)).replace(day=1)

    if not all_dfs:
        print("未能取得任何資料。")
        return

    df = pd.concat(all_dfs).reset_index(drop=True)
    df['student_level'] = pd.Categorical(df['student_level'], categories=levels, ordered=True)
    df = df.sort_values(['month', 'student_level'])

    pivot_instances = df.pivot_table(index='month', columns='student_level', values='usage_instances', fill_value=0, aggfunc='sum')
    pivot_users = df.pivot_table(index='month', columns='student_level', values='usage_users', fill_value=0, aggfunc='sum')

    for level in levels:
        if level not in pivot_instances.columns:
            pivot_instances[level] = 0
        if level not in pivot_users.columns:
            pivot_users[level] = 0

    pivot_instances = pivot_instances[levels].sort_index()
    pivot_users = pivot_users[levels].sort_index()
    pivot_instances['總計'] = pivot_instances.sum(axis=1)
    pivot_users['總計'] = pivot_users[levels].sum(axis=1)

    batch_date = datetime.now().date()
    stats_data = []
    for month in pivot_instances.index:
        for level in levels + ['總計']:
            cum = cumulative_map.get(month, 0) if level == '總計' else 0
            stats_data.append({
                'month': month,
                'student_level': level,
                'usage_instances': int(pivot_instances.loc[month, level]),
                'usage_users': int(pivot_users.loc[month, level]),
                'cumulative_users': cum
            })

    from backend.database.service_lab import ServiceLAB
    from backend.database.service_ncu import ServiceNCU

    for name, svc_cls in [('LAB', ServiceLAB), ('NCU', ServiceNCU)]:
        try:
            db = svc_cls()
            db.createTable_usage_stats()
            n = db.insert_usage_stats(stats_data, batch_date)
            db.cursor.close()
            db.db.close()
            print(f"成功寫入 {n} 筆每月統計到 {name} 資料庫")
        except Exception as e:
            print(f"寫入 {name} 失敗: {e}")
            import traceback
            traceback.print_exc()


def main():
    parser = argparse.ArgumentParser(description='使用統計（daily / monthly）')
    parser.add_argument('--mode', choices=['daily', 'monthly'], default='daily', help='daily=每日, monthly=每月')
    args = parser.parse_args()

    if args.mode == 'daily':
        run_daily()
    else:
        run_monthly()


if __name__ == '__main__':
    main()
