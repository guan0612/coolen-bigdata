import mysql.connector
import pandas as pd
from datetime import datetime
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

# ==================== 資料庫連線設定 ====================
config = {
    'host': Config.LAB_DB_HOST,
    'port': Config.LAB_DB_PORT,
    'user': Config.LAB_DB_USERNAME,
    'password': Config.LAB_DB_PASSWORD,
    'database': Config.LAB_DB_NAME,
    'charset': 'utf8mb4',
    'connection_timeout': 30
}

# ==================== SQL 查詢（大數據） ====================
query = """
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
    COUNT(DISTINCT actor.uid) AS user_count
FROM client_event
LEFT JOIN action ON client_event.action_id = action.id
LEFT JOIN actor ON client_event.actor_id = actor.id
WHERE 
    client_event.client_event_time IS NOT NULL
    AND client_event.client_event_time >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
    AND action.course_id IN (
        '770', '772', '775', '779',
        '941', '942', '943', '944',
        '767', '768', '771', '773', '774', '776', '777', '778', '918', '919', '920', '921', '1358',
        '841', '843', '922', '923', '924', '925',
        '842', '844', '926', '927', '928', '930',
        '862', '864', '868', '937', '938', '939', '940', '1355', '1388',
        '989', '990', '991', '992',
        '912', '915', '916', '917',
        '972', '973', '974', '975'
    )
    AND actor.category IS NOT NULL
    AND actor.category IN ('國小', '國中', '普高', '技高', '大專')
GROUP BY ai_course_name, student_level, month
ORDER BY ai_course_name, month, 
    FIELD(actor.category, '國小', '國中', '普高', '技高', '大專')
"""

# ==================== 連接資料庫並查詢 ====================
print("正在連接 LAB 資料庫...")
print(f"連線資訊: {Config.LAB_DB_HOST}:{Config.LAB_DB_PORT}/{Config.LAB_DB_NAME}")

try:
    conn = mysql.connector.connect(**config)
    print("✅ 資料庫連線成功！")
    
    # 設定 session timeout（避免查詢時間過長導致連線中斷）
    cursor = conn.cursor()
    cursor.execute("SET SESSION net_read_timeout = 3600")
    cursor.execute("SET SESSION net_write_timeout = 3600")
    cursor.execute("SET SESSION wait_timeout = 28800")
    conn.commit()
    print("✅ Session timeout 設定完成（net_read=3600s, net_write=3600s, wait=28800s）")
    
    # 測試連線
    cursor.execute("SELECT VERSION()")
    version = cursor.fetchone()
    print(f"MySQL 版本: {version[0]}")
    cursor.close()
    
    # 執行查詢
    print("正在執行查詢...")
    df = pd.read_sql(query, conn)
    print(f"✅ 查詢完成，共 {len(df)} 筆資料")
    
    # 儲存原始資料供後續使用（只保留月份的統計）
    stats_df = df[df['month'].notna() & (df['month'] != '')].copy()
    
    if stats_df.empty:
        print("⚠️ 沒有統計資料可寫入資料庫")
        sys.exit(0)
    
except mysql.connector.Error as err:
    print(f"❌ 資料庫連線錯誤: {err}")
    sys.exit(1)
    
except Exception as e:
    print(f"❌ 其他錯誤: {e}")
    sys.exit(1)
    
finally:
    if 'conn' in locals() and conn.is_connected():
        conn.close()
        print("資料庫連線已關閉")

# ==================== 寫入資料庫 ====================
if 'stats_df' in locals() and not stats_df.empty:
    # 準備資料（只保留月份的統計）
    batch_date = datetime.now().date()
    stats_data = stats_df.to_dict('records')
    
    # 寫入 LAB 資料庫
    print("\n正在連接 LAB 資料庫...")
    try:
        db_lab = ServiceLAB()
        print("✅ LAB 資料庫連線成功！")
        
        # 建立表（如果不存在）
        print("正在建立統計表...")
        db_lab.createTable_ai_course_stats()
        print("✅ 統計表建立/檢查完成")
        
        # 插入資料
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
        
        # 建立表（如果不存在）
        print("正在建立統計表...")
        db_ncu.createTable_ai_course_stats()
        print("✅ 統計表建立/檢查完成")
        
        # 插入資料
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
