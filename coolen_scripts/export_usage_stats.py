import mysql.connector
import pandas as pd
from datetime import datetime
import sys

# 添加應用程式路徑
sys.path.insert(0, '/app')

from backend.database.config import Config

config = {
    'host': Config.LAB_DB_HOST,
    'port': Config.LAB_DB_PORT,
    'user': Config.LAB_DB_USERNAME,
    'password': Config.LAB_DB_PASSWORD,
    'database': Config.LAB_DB_NAME,
    'charset': 'utf8mb4'
}

usage_query = """
SELECT 
    uid_month.month,
    uid_month.primary_level AS student_level,
    SUM(event_counts.usage_instances) AS usage_instances,
    COUNT(DISTINCT uid_month.uid) AS usage_users
FROM (
    -- 為每個 UID 在每個月份分配一個主要學制（優先順序：其他 > 高中職 > 國中 > 國小）
    SELECT 
        month,
        uid,
        MIN(CASE 
            WHEN category_level = '其他' THEN 1
            WHEN category_level = '高中職' THEN 2
            WHEN category_level = '國中' THEN 3
            ELSE 4
        END) AS priority,
        MIN(CASE 
            WHEN category_level = '其他' THEN '其他'
            WHEN category_level = '高中職' THEN '高中職'
            WHEN category_level = '國中' THEN '國中'
            ELSE '國小'
        END) AS primary_level
    FROM (
        SELECT 
            DATE_FORMAT(FROM_UNIXTIME(CAST(SUBSTRING_INDEX(ce.server_sign_token, ':', -1) AS UNSIGNED) + 28800), '%Y-%m') AS month,
            actor.uid AS uid,
            CASE 
                WHEN actor.category = '國小' THEN '國小'
                WHEN actor.category = '國中' THEN '國中'
                WHEN actor.category IN ('普高', '技高') THEN '高中職'
                ELSE '其他'
            END AS category_level
        FROM client_event AS ce
        JOIN actor ON ce.actor_id = actor.id
        WHERE ce.server_sign_token IS NOT NULL 
          AND ce.server_sign_token != ''
          AND ce.server_sign_token REGEXP '^[^:]+:[^:]+:[0-9]+$'
    ) AS base
    GROUP BY month, uid
) AS uid_month
JOIN (
    -- 計算每個月份、每個 UID 的所有事件數量（不區分學制）
    SELECT 
        DATE_FORMAT(FROM_UNIXTIME(CAST(SUBSTRING_INDEX(ce.server_sign_token, ':', -1) AS UNSIGNED) + 28800), '%Y-%m') AS month,
        actor.uid AS uid,
        COUNT(*) AS usage_instances
    FROM client_event AS ce
    JOIN actor ON ce.actor_id = actor.id
    WHERE ce.server_sign_token IS NOT NULL 
      AND ce.server_sign_token != ''
      AND ce.server_sign_token REGEXP '^[^:]+:[^:]+:[0-9]+$'
    GROUP BY month, uid
) AS event_counts ON uid_month.month = event_counts.month 
    AND uid_month.uid = event_counts.uid
GROUP BY uid_month.month, uid_month.primary_level
ORDER BY uid_month.month, FIELD(uid_month.primary_level, '國小', '國中', '高中職', '其他');
"""

print("正在連接 LAB 資料庫...")
print(f"連線資訊: {Config.LAB_DB_HOST}:{Config.LAB_DB_PORT}/{Config.LAB_DB_NAME}")

try:
    conn = mysql.connector.connect(**config)
    print("✅ 資料庫連線成功！")
    df = pd.read_sql(usage_query, conn)
    print(f"✅ 查詢完成，共 {len(df)} 筆資料")
    if not df.empty:
        print("前 10 筆資料預覽：")
        print(df.head(10))
finally:
    if 'conn' in locals() and conn.is_connected():
        conn.close()
        print("資料庫連線已關閉")

if df.empty:
    print("⚠️ 沒有資料可處理")
    sys.exit(0)

levels = ['國小', '國中', '高中職', '其他']

pivot_instances = df.pivot_table(index='month', columns='student_level', values='usage_instances', fill_value=0, aggfunc='sum')
pivot_users = df.pivot_table(index='month', columns='student_level', values='usage_users', fill_value=0, aggfunc='sum')

for level in levels:
    if level not in pivot_instances.columns:
        pivot_instances[level] = 0
    if level not in pivot_users.columns:
        pivot_users[level] = 0

pivot_instances = pivot_instances[levels].sort_index()
pivot_users = pivot_users[levels].sort_index()

# 計算總計（人次和人數都直接加總，因為每個 UID 只會被計入一個學制）
pivot_instances['總計'] = pivot_instances.sum(axis=1)
pivot_users['總計'] = pivot_users[levels].sum(axis=1)

# ==================== 計算累計使用人數 ====================
print("\n正在計算累計使用人數...")
try:
    conn2 = mysql.connector.connect(**config)
    cumulative_dict = {}
    
    # 對每個月份，查詢從開始到該月份為止的所有不重複使用者
    for month in sorted(pivot_users.index):
        # 計算累計人數（從開始到該月份）
        month_cumulative_query = f"""
        SELECT COUNT(DISTINCT actor.uid) AS cumulative_users
        FROM client_event AS ce
        JOIN actor ON ce.actor_id = actor.id
        WHERE ce.server_sign_token IS NOT NULL 
          AND ce.server_sign_token != ''
          AND ce.server_sign_token REGEXP '^[^:]+:[^:]+:[0-9]+$'
          AND DATE_FORMAT(FROM_UNIXTIME(CAST(SUBSTRING_INDEX(ce.server_sign_token, ':', -1) AS UNSIGNED) + 28800), '%Y-%m') <= '{month}'
        """
        
        cursor = conn2.cursor()
        cursor.execute(month_cumulative_query)
        result = cursor.fetchone()
        cumulative_dict[month] = result[0] if result else 0
        cursor.close()
    
    conn2.close()
    
    # 將累計人數加入 pivot_users
    pivot_users['累計'] = pivot_users.index.map(cumulative_dict).fillna(0).astype(int)
    print("✅ 累計使用人數計算完成")
except Exception as e:
    print(f"⚠️ 計算累計使用人數時發生錯誤，使用累加方式: {e}")
    import traceback
    traceback.print_exc()
    # 如果計算失敗，使用累加方式（不準確但可用）
    pivot_users['累計'] = pivot_users['總計'].cumsum()

# ==================== 顯示統計摘要 ====================
print(f"\n📅 月份範圍：{pivot_instances.index.min()} ~ {pivot_instances.index.max()}")
print(f"📊 總使用人次：{int(pivot_instances['總計'].sum()):,}")
print(f"👥 總使用人數（單月最大值）：{int(pivot_users['總計'].max()):,}")
if '累計' in pivot_users.columns:
    print(f"📈 累計使用人數（最後一個月）：{int(pivot_users['累計'].iloc[-1]):,}")

# ==================== 寫入資料庫 ====================
print("\n正在寫入資料庫...")
try:
    from backend.database.service_lab import ServiceLAB
    from backend.database.service_ncu import ServiceNCU
    
    # 準備資料
    batch_date = datetime.now().date()
    stats_data = []
    
    for month in pivot_instances.index:
        for level in levels + ['總計']:
            cumulative = int(pivot_users.loc[month, '累計']) if level == '總計' and '累計' in pivot_users.columns else 0
            stats_data.append({
                'month': month,
                'student_level': level,
                'usage_instances': int(pivot_instances.loc[month, level]),
                'usage_users': int(pivot_users.loc[month, level]),
                'cumulative_users': cumulative
            })
    
    # 寫入 LAB 資料庫
    print("正在連接 LAB 資料庫...")
    db_lab = ServiceLAB()
    db_lab.createTable_usage_stats()
    inserted_count_lab = db_lab.insert_usage_stats(stats_data, batch_date)
    print(f"✅ 成功寫入 {inserted_count_lab} 筆統計資料到 LAB 資料庫")
    db_lab.cursor.close()
    db_lab.db.close()
    
    # 寫入 NCU 資料庫
    print("正在連接 NCU 資料庫...")
    db_ncu = ServiceNCU()
    db_ncu.createTable_usage_stats()
    inserted_count_ncu = db_ncu.insert_usage_stats(stats_data, batch_date)
    print(f"✅ 成功寫入 {inserted_count_ncu} 筆統計資料到 NCU 資料庫")
    db_ncu.cursor.close()
    db_ncu.db.close()
    
except Exception as e:
    print(f"⚠️ 寫入資料庫時發生錯誤: {e}")
    import traceback
    traceback.print_exc()
