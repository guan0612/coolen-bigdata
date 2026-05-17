import mysql.connector
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

# 添加應用程式路徑（不要寫死 /app，避免離開 Docker 後無法執行）
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# 導入資料庫配置
from backend.database.config import Config
from backend.database.service_lab import ServiceLAB
from backend.database.service_ncu import ServiceNCU

# ==================== 資料庫連線設定（統計查詢來源：NCU） ====================
config = {
    'host': Config.NCU_DB_HOST,
    'port': Config.NCU_DB_PORT,
    'user': Config.NCU_DB_USERNAME,
    'password': Config.NCU_DB_PASSWORD,
    'database': Config.NCU_DB_NAME,
    'charset': 'utf8mb4',
    'connection_timeout': 30
}

COURSE_IDS = """'770', '772', '775', '779',
    '941', '942', '943', '944',
    '767', '768', '771', '773', '774', '776', '777', '778', '918', '919', '920', '921', '1358',
    '841', '843', '922', '923', '924', '925',
    '842', '844', '926', '927', '928', '930',
    '862', '864', '868', '937', '938', '939', '940', '1355', '1388',
    '989', '990', '991', '992',
    '912', '915', '916', '917',
    '972', '973', '974', '975'"""


def get_daily_query(day_start: str, day_end: str) -> str:
    """單日查詢，避免一次查 3 個月或整月導致逾時。"""
    return f"""
SELECT 
    CASE 
        WHEN action.course_id IN ('770', '772', '775', '779') THEN '酷英AI英語聊天機器人'
        WHEN action.course_id IN ('941', '942', '943', '944') THEN '酷英篇章口說評測系統'
        WHEN action.course_id IN ('767', '768', '771', '773', '774', '776', '777', '778', '918', '919', '920', '921', '1358') THEN '酷英語音合成工具/語音合成工具'
        WHEN action.course_id IN ('841', '843', '922', '923', '924', '925') THEN '酷英AI寫作偵錯工具'
        WHEN action.course_id IN ('842', '844', '926', '927', '928', '930') THEN '多益寫作評估工具'
        WHEN action.course_id IN ('862', '864', '868', '937', '938', '939', '940', '1355', '1388') THEN 'Linggle Write'
        WHEN action.course_id IN ('989', '990', '991', '992') THEN '酷英教學＆學習工具區'
        WHEN action.course_id IN ('912', '915', '916', '917') THEN '酷英教師AI特助'
        WHEN action.course_id IN ('972', '973', '974', '975') THEN '酷英沉浸式閱讀工具'
    END AS ai_course_name,
    actor.category AS student_level,
    DATE_FORMAT(client_event.client_event_time, '%Y-%m') AS month,
    actor.uid
FROM client_event
LEFT JOIN action ON client_event.action_id = action.id
LEFT JOIN actor ON client_event.actor_id = actor.id
WHERE 
    client_event.client_event_time IS NOT NULL
    AND client_event.client_event_time >= '{day_start}'
    AND client_event.client_event_time < '{day_end}'
    AND action.course_id IN (
        {COURSE_IDS}
    )
    AND actor.category IS NOT NULL
    AND actor.category IN ('國小', '國中', '普高', '技高', '大專')
    AND actor.uid != '1'
GROUP BY ai_course_name, student_level, month, actor.uid
"""


# ==================== 連接資料庫並查詢（按日分批） ====================
print("正在連接 NCU 資料庫（統計查詢來源）...")
print(f"連線資訊: {Config.NCU_DB_HOST}:{Config.NCU_DB_PORT}/{Config.NCU_DB_NAME}")

import urllib.parse
from sqlalchemy import create_engine, text
safe_password = urllib.parse.quote_plus(Config.NCU_DB_PASSWORD)
DB_URL = f"mysql+mysqlconnector://{Config.NCU_DB_USERNAME}:{safe_password}@{Config.NCU_DB_HOST}:{Config.NCU_DB_PORT}/{Config.NCU_DB_NAME}"
engine = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=3600)

stats_df = None

try:
    print("✅ 資料庫連線引擎建立成功！")

    # 計算近 3 個月的月份範圍
    today = datetime.now().date()
    start_dt = (today - timedelta(days=93)).replace(day=1)  # 約 3 個月前
    end_dt = (today + timedelta(days=32)).replace(day=1)

    all_dfs = []
    current_day = start_dt
    while current_day < end_dt and current_day <= today:
        day_start = current_day.strftime('%Y-%m-%d')
        day_end = (current_day + timedelta(days=1)).strftime('%Y-%m-%d')

        try:
            with engine.connect() as conn:
                conn.execute(text("SET SESSION net_read_timeout = 3600"))
                query = get_daily_query(day_start, day_end)
                df = pd.read_sql(text(query), conn)
                if not df.empty:
                    all_dfs.append(df)
            if current_day.day % 7 == 0 or current_day == today:
                print(f"  已完成抓取至: {day_start}")
        except Exception as e:
            print(f"  ❌ 查詢 {day_start} 失敗: {e}")
        current_day += timedelta(days=1)

    if not all_dfs:
        print("⚠️ 沒有統計資料可寫入資料庫")
        stats_df = pd.DataFrame()
    else:
        df = pd.concat(all_dfs, ignore_index=True)
        stats_df = df.groupby(['ai_course_name', 'student_level', 'month'])['uid'].nunique().reset_index()
        stats_df.rename(columns={'uid': 'user_count'}, inplace=True)
        stats_df = stats_df[stats_df['month'].notna() & (stats_df['month'] != '')].copy()
        
        # 進行排序
        cat_order = ['國小', '國中', '普高', '技高', '大專']
        stats_df['student_level'] = pd.Categorical(stats_df['student_level'], categories=cat_order, ordered=True)
        stats_df = stats_df.sort_values(['ai_course_name', 'month', 'student_level']).reset_index(drop=True)
        
        print(f"✅ 查詢完成，共 {len(stats_df)} 筆彙整資料")

except Exception as e:
    print(f"❌ 錯誤: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ==================== 寫入資料庫 ====================
if stats_df is not None and not stats_df.empty:
    batch_date = datetime.now().date()
    stats_data = stats_df.to_dict('records')

    # 寫入 LAB 資料庫
    print("\n正在連接 LAB 資料庫...")
    try:
        db_lab = ServiceLAB()
        print("✅ LAB 資料庫連線成功！")
        print("正在建立統計表...")
        db_lab.createTable_ai_course_stats()
        print("✅ 統計表建立/檢查完成")
        print(f"正在寫入統計資料到 LAB 資料庫（批次日期：{batch_date}）...")
        inserted_count_lab = db_lab.insert_ai_course_stats(stats_data, batch_date)
        print(f"✅ 成功寫入 {inserted_count_lab} 筆統計資料到 LAB 資料庫")
        db_lab.cursor.close()
        db_lab.db.close()
    except Exception as e:
        print(f"⚠️ 寫入 LAB 資料庫時發生錯誤: {e}")
        import traceback
        traceback.print_exc()

    # 寫入 NCU 資料庫
    print("\n正在連接 NCU 資料庫...")
    try:
        db_ncu = ServiceNCU()
        print("✅ NCU 資料庫連線成功！")
        print("正在建立統計表...")
        db_ncu.createTable_ai_course_stats()
        print("✅ 統計表建立/檢查完成")
        print(f"正在寫入統計資料到 NCU 資料庫（批次日期：{batch_date}）...")
        inserted_count_ncu = db_ncu.insert_ai_course_stats(stats_data, batch_date)
        print(f"✅ 成功寫入 {inserted_count_ncu} 筆統計資料到 NCU 資料庫")
        db_ncu.cursor.close()
        db_ncu.db.close()
    except Exception as e:
        print(f"⚠️ 寫入 NCU 資料庫時發生錯誤: {e}")
        import traceback
        traceback.print_exc()
else:
    print("⚠️ 沒有統計資料可寫入資料庫")
