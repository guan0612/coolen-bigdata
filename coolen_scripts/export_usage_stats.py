"""
使用統計：支援 daily / monthly 兩種模式。
統計範圍固定為 2025-08-01 至目前，避免錯誤的 client_event_time 影響累計。

  --mode daily  : 寫入 usage_stats_daily（近 3 天，僅統計當日）
  --mode monthly: 寫入 usage_stats（每月人次、人數、累計）
"""
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

# 統計範圍起點（避免錯誤資料影響累計）
STAT_RANGE_START = '2025-08-01'

# 統計查詢來源：NCU（寫入仍由 ServiceLAB / ServiceNCU 寫入兩邊統計表）
_safe_ncu_pw = urllib.parse.quote_plus(Config.NCU_DB_PASSWORD)
DB_URL = (
    f"mysql+mysqlconnector://{Config.NCU_DB_USERNAME}:{_safe_ncu_pw}"
    f"@{Config.NCU_DB_HOST}:{Config.NCU_DB_PORT}/{Config.NCU_DB_NAME}"
)
engine = create_engine(DB_URL)

levels = ['國小', '國中', '高中職', '其他']
DEFAULT_USAGE_ACTION_ACTIVITY = "ContentLoaded"


def _build_activity_filter_sql(activity_filter):
    if activity_filter:
        return f" AND ce.action_activity = '{activity_filter}'"
    return ""


def get_daily_query(start_date, end_date, activity_filter):
    """依日彙總的查詢（範圍限制在 STAT_RANGE_START 之後）"""
    start = max(start_date, STAT_RANGE_START)
    activity_sql = _build_activity_filter_sql(activity_filter)
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
          {activity_sql}
          AND a.uid != '1'
        GROUP BY day_label, a.uid
    ) AS sub
    GROUP BY stat_date, student_level
    """

def get_monthly_query(start_date, end_date, activity_filter):
    """依月彙總的查詢"""
    activity_sql = _build_activity_filter_sql(activity_filter)
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
          {activity_sql}
          AND a.uid != '1'
        GROUP BY month_label, a.uid
    ) AS sub
    GROUP BY month, student_level
    """


def get_monthly_uids_query(month_start, month_end, activity_filter):
    """查詢單月內的不重複 uid（用於累積人數計算）"""
    activity_sql = _build_activity_filter_sql(activity_filter)
    return f"""
    SELECT DISTINCT a.uid
    FROM client_event AS ce
    JOIN actor AS a ON ce.actor_id = a.id
    WHERE ce.client_event_time >= '{month_start}'
      AND ce.client_event_time < '{month_end}'
      {activity_sql}
      AND a.uid != '1'
    """


def run_daily(activity_filter):
    """每日統計：只統計當日（非 MTD），寫入 usage_stats_daily"""
    today = datetime.now().date()
    start_dt = today - timedelta(days=30)
    end_dt = today + timedelta(days=1)

    print(f"每日統計（只統計當日，範圍 {STAT_RANGE_START} ~ 目前）")
    print(f"處理日期：{start_dt} ~ {today}...")

    all_dfs = []
    current = start_dt
    while current < end_dt:
        batch_start_str = current.strftime('%Y-%m-%d')
        batch_end_str = (current + timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"  處理 {batch_start_str}...")
        try:
            with engine.connect() as conn:
                conn.execute(text("SET SESSION net_read_timeout = 36000"))
                conn.execute(text("SET SESSION net_write_timeout = 36000"))
                df = pd.read_sql_query(text(get_daily_query(batch_start_str, batch_end_str, activity_filter)), conn)
            if not df.empty:
                all_dfs.append(df)
        except Exception as e:
            print(f"  查詢失敗: {e}")
        current += timedelta(days=1)

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


def run_monthly(activity_filter):
    """每月統計：優化版（按天分批抓取，避免 MySQL 斷線）"""
    today = datetime.now().date()
    # 統計起點：2025-08-01
    start_dt = datetime.strptime(STAT_RANGE_START, '%Y-%m-%d').date()
    # 統計終點：明天凌晨（包含今天完整的資料）
    end_dt = today + timedelta(days=1)

    print(f"--- 每月統計啟動 (範圍: {start_dt} ~ {today}) ---")

    all_day_data = []      # 存每一天的統計結果
    monthly_uid_priorities = {} # 存每個月的 UID 及其最高優先級身分 { "2025-08": { uid: min_priority } }
    cumulative_set = set() # 存開站至今所有出現過的 UID
    cumulative_map = {}    # 存每個月底的累計人數

    priority_map = {1: '其他', 2: '高中職', 3: '國中', 4: '國小'}

    current = start_dt
    while current < end_dt:
        day_str = current.strftime('%Y-%m-%d')
        day_next = (current + timedelta(days=1)).strftime('%Y-%m-%d')
        month_label = current.strftime('%Y-%m')

        # 初始化該月的 UID 優先級字典
        if month_label not in monthly_uid_priorities:
            monthly_uid_priorities[month_label] = {}

        try:
            with engine.connect() as conn:
                # 1. 抓取當天的統計數據 (Instances)
                # 使用你原本的 get_daily_query，但傳入當天範圍
                query = get_daily_query(day_str, day_next, activity_filter)
                df_day = pd.read_sql_query(text(query), conn)
                
                # 2. 抓取當天的所有 UID 及其優先級 (用於計算精確的月人數與累計)
                uid_sql = f"""
                    SELECT a.uid,
                           MIN(CASE 
                               WHEN a.category = '國小' THEN 4
                               WHEN a.category = '國中' THEN 3
                               WHEN a.category IN ('普高', '技高') THEN 2
                               ELSE 1
                           END) AS min_priority
                    FROM client_event ce 
                    JOIN actor a ON ce.actor_id = a.id 
                    WHERE ce.client_event_time >= '{day_str}' 
                      AND ce.client_event_time < '{day_next}' 
                      {_build_activity_filter_sql(activity_filter)}
                      AND a.uid != '1'
                    GROUP BY a.uid
                """
                df_uids = pd.read_sql_query(text(uid_sql), conn)
            
            # 處理數據
            if not df_day.empty:
                df_day['month'] = month_label # 標註月份
                all_day_data.append(df_day)
            
            day_uids = set()
            for _, row in df_uids.iterrows():
                u_id = str(row['uid'])
                prio = int(row['min_priority'])
                day_uids.add(u_id)
                # 更新該月內該 UID 的最高優先級（取 MIN，因為 SQL 邏輯中越小代表某些特定優先級，需與 get_daily_query 保持一致）
                if u_id not in monthly_uid_priorities[month_label] or prio < monthly_uid_priorities[month_label][u_id]:
                    monthly_uid_priorities[month_label][u_id] = prio

            cumulative_set |= day_uids            # 併入總累計集合
            
            # 更新當前月份的累計人數 (會隨著每一天更新，最後停在月底數字)
            cumulative_map[month_label] = len(cumulative_set)

            if current.day == 1 or current == today:
                print(f"  [進度] 正在處理: {day_str} ...")

        except Exception as e:
            print(f"  [錯誤] {day_str} 查詢失敗: {e}")

        current += timedelta(days=1)

    if not all_day_data:
        print("❌ 未能取得任何資料。")
        return

    # --- 資料彙整 (Pandas 處理) ---
    full_df = pd.concat(all_day_data).reset_index(drop=True)
    
    # 依月份與身分加總人次
    pivot_instances = full_df.pivot_table(
        index='month', columns='student_level', 
        values='usage_instances', aggfunc='sum', fill_value=0
    )

    # 處理不重複人數 (計算每月各身分的不重複人數)
    monthly_user_rows = []
    for month_label, uid_prios in monthly_uid_priorities.items():
        for u_id, prio in uid_prios.items():
            level = priority_map.get(prio, '國小')
            monthly_user_rows.append({'month': month_label, 'student_level': level, 'uid': u_id})
    
    if monthly_user_rows:
        df_monthly_users = pd.DataFrame(monthly_user_rows)
        pivot_users = df_monthly_users.pivot_table(
            index='month', columns='student_level', 
            values='uid', aggfunc='nunique', fill_value=0
        )
    else:
        pivot_users = pd.DataFrame(columns=levels)

    # 計算總計人數（直接加總各身分，因為每個 UID 在當月只會被歸類到一個身分）
    pivot_users['總計'] = pivot_users.sum(axis=1)

    # 補齊欄位與排序
    for col in levels:
        if col not in pivot_instances.columns: pivot_instances[col] = 0
        if col not in pivot_users.columns: pivot_users[col] = 0
    
    pivot_instances = pivot_instances[levels].sort_index()
    pivot_instances['總計'] = pivot_instances.sum(axis=1)
    pivot_users = pivot_users[levels + ['總計']].sort_index()

    # --- 寫入資料庫 ---
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

    # 調用 ServiceLAB / ServiceNCU 寫入 (沿用你原本的邏輯)
    from backend.database.service_lab import ServiceLAB
    from backend.database.service_ncu import ServiceNCU

    for name, svc_cls in [('LAB', ServiceLAB), ('NCU', ServiceNCU)]:
        try:
            db = svc_cls()
            db.createTable_usage_stats()
            n = db.insert_usage_stats(stats_data, today)
            db.db.close()
            print(f"✅ 成功寫入 {n} 筆每月統計到 {name}")
        except Exception as e:
            print(f"❌ 寫入 {name} 失敗: {e}")

def main():
    parser = argparse.ArgumentParser(description='使用統計（daily / monthly）')
    parser.add_argument('--mode', choices=['daily', 'monthly'], default='daily', help='daily=每日, monthly=每月')
    parser.add_argument(
        '--activity-filter',
        choices=['ContentLoaded', 'ALL'],
        default=DEFAULT_USAGE_ACTION_ACTIVITY,
        help='ContentLoaded=只統計 ContentLoaded；ALL=不做 action_activity 過濾（原本版本）'
    )
    args = parser.parse_args()
    activity_filter = None if args.activity_filter == 'ALL' else args.activity_filter

    if args.mode == 'daily':
        run_daily(activity_filter)
    else:
        run_monthly(activity_filter)


if __name__ == '__main__':
    main()
