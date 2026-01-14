import mysql.connector
import pandas as pd
from datetime import datetime
import sys
import os

# 添加應用程式路徑
sys.path.insert(0, '/app')

# 導入資料庫配置
from backend.database.config import Config

# ==================== 資料庫連線設定 ====================
config = {
    'host': Config.LAB_DB_HOST,
    'port': Config.LAB_DB_PORT,
    'user': Config.LAB_DB_USERNAME,
    'password': Config.LAB_DB_PASSWORD,
    'database': Config.LAB_DB_NAME,
    'charset': 'utf8mb4'
}

# ==================== SQL 查詢（登入統計） ====================
# 當同一個 UID 有多個學制時，取最高優先順序的學制（優先順序：其他 > 高中職 > 國中 > 國小）
# 時間依據：每個 session 最早出現的 server_sign_token 時間戳記（台灣時間 UTC+8）
# 每個 session 只計入一次，根據該 session 最早出現的時間歸類到對應月份
query = """
SELECT 
    session_first.month,
    uid_month.primary_level AS student_level,
    COUNT(DISTINCT session_first.session) AS login_instances,
    COUNT(DISTINCT session_first.uid) AS login_users
FROM (
    -- 找出每個 session 最早出現的時間和對應的學制
    -- 如果同一個 session 有多個學制，取優先順序最高的學制（優先順序：其他 > 高中職 > 國中 > 國小）
    SELECT 
        DATE_FORMAT(FROM_UNIXTIME(MIN(CAST(SUBSTRING_INDEX(ce.server_sign_token, ':', -1) AS UNSIGNED)) + 28800), '%Y-%m') AS month,
        actor.uid AS uid,
        actor.session AS session,
        MIN(CASE 
            WHEN actor.category = '國小' THEN 4
            WHEN actor.category = '國中' THEN 3
            WHEN actor.category IN ('普高', '技高') THEN 2
            ELSE 1
        END) AS priority,
        MIN(CASE 
            WHEN actor.category = '國小' THEN '國小'
            WHEN actor.category = '國中' THEN '國中'
            WHEN actor.category IN ('普高', '技高') THEN '高中職'
            ELSE '其他'
        END) AS session_category_level
    FROM client_event AS ce
    JOIN actor ON ce.actor_id = actor.id
    WHERE ce.server_sign_token IS NOT NULL 
      AND ce.server_sign_token != ''
      AND ce.server_sign_token REGEXP '^[^:]+:[^:]+:[0-9]+$'
      AND actor.session IS NOT NULL
      AND actor.session != ''
    GROUP BY actor.uid, actor.session
) AS session_first
JOIN (
    -- 為每個 UID 在每個月份分配一個主要學制（優先順序：其他 > 高中職 > 國中 > 國小）
    -- 使用該月份內該 UID 所有 session 的學制中優先順序最高的
    SELECT 
        month,
        uid,
        MIN(priority) AS min_priority,
        MIN(CASE 
            WHEN priority = 1 THEN '其他'
            WHEN priority = 2 THEN '高中職'
            WHEN priority = 3 THEN '國中'
            ELSE '國小'
        END) AS primary_level
    FROM (
        SELECT 
            DATE_FORMAT(FROM_UNIXTIME(MIN(CAST(SUBSTRING_INDEX(ce.server_sign_token, ':', -1) AS UNSIGNED)) + 28800), '%Y-%m') AS month,
            actor.uid AS uid,
            MIN(CASE 
                WHEN actor.category = '國小' THEN 4
                WHEN actor.category = '國中' THEN 3
                WHEN actor.category IN ('普高', '技高') THEN 2
                ELSE 1
            END) AS priority
        FROM client_event AS ce
        JOIN actor ON ce.actor_id = actor.id
        WHERE ce.server_sign_token IS NOT NULL 
          AND ce.server_sign_token != ''
          AND ce.server_sign_token REGEXP '^[^:]+:[^:]+:[0-9]+$'
          AND actor.session IS NOT NULL
          AND actor.session != ''
        GROUP BY actor.uid, actor.session
    ) AS base
    GROUP BY month, uid
) AS uid_month ON session_first.month = uid_month.month 
    AND session_first.uid = uid_month.uid
GROUP BY session_first.month, uid_month.primary_level
ORDER BY session_first.month, FIELD(uid_month.primary_level, '國小', '國中', '高中職', '其他')
"""

# ==================== 連接資料庫並查詢 ====================
print("正在連接 LAB 資料庫...")
print(f"連線資訊: {Config.LAB_DB_HOST}:{Config.LAB_DB_PORT}/{Config.LAB_DB_NAME}")

try:
    conn = mysql.connector.connect(**config)
    print("✅ 資料庫連線成功！")
    
    # 測試連線
    cursor = conn.cursor()
    cursor.execute("SELECT VERSION()")
    version = cursor.fetchone()
    print(f"MySQL 版本: {version[0]}")
    cursor.close()
    
    # 執行查詢
    print("正在執行查詢...")
    df = pd.read_sql(query, conn)
    print(f"✅ 查詢完成，共 {len(df)} 筆資料")
    
    # 顯示前幾筆資料
    if len(df) > 0:
        print("\n前 10 筆資料預覽：")
        print(df.head(10))
    
except mysql.connector.Error as err:
    print(f"❌ 資料庫連線錯誤: {err}")
    sys.exit(1)
    
except Exception as e:
    print(f"❌ 其他錯誤: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
    
finally:
    if 'conn' in locals() and conn.is_connected():
        conn.close()
        print("資料庫連線已關閉")

# ==================== 資料處理 ====================
if df.empty:
    print("⚠️ 沒有資料可處理")
    sys.exit(0)

# 過濾掉月份為空或無效的資料
df = df[df['month'].notna() & (df['month'] != '')].copy()

if df.empty:
    print("⚠️ 過濾後沒有有效資料")
    sys.exit(0)

# 建立透視表：月份 x 學制
pivot_instances = df.pivot_table(
    index='month',
    columns='student_level',
    values='login_instances',
    fill_value=0,
    aggfunc='sum'
)

pivot_users = df.pivot_table(
    index='month',
    columns='student_level',
    values='login_users',
    fill_value=0,
    aggfunc='sum'
)

# 確保所有學制都存在
all_levels = ['國小', '國中', '高中職', '其他']
for level in all_levels:
    if level not in pivot_instances.columns:
        pivot_instances[level] = 0
    if level not in pivot_users.columns:
        pivot_users[level] = 0

# 重新排序欄位
pivot_instances = pivot_instances[all_levels]
pivot_users = pivot_users[all_levels]

# 計算總計（人次和人數都直接加總，因為每個 UID 只會被計入一個學制）
pivot_instances['總計'] = pivot_instances.sum(axis=1)
pivot_users['總計'] = pivot_users[all_levels].sum(axis=1)

# 計算累計登入人數（從開始到該月份為止的不重複使用者總數）
print("\n正在計算累計登入人數...")
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
    print("✅ 累計登入人數計算完成")
except Exception as e:
    print(f"⚠️ 計算累計人數時發生錯誤，使用累加方式: {e}")
    import traceback
    traceback.print_exc()
    # 如果計算失敗，使用累加方式（不準確但可用）
    pivot_users['累計'] = pivot_users['總計'].cumsum()

# 排序月份
pivot_instances = pivot_instances.sort_index()
pivot_users = pivot_users.sort_index()

# ==================== 顯示統計摘要 ====================
print(f"\n📊 月份範圍：{pivot_instances.index[0]} ~ {pivot_instances.index[-1]}")
print(f"📅 總月份數：{len(pivot_instances)}")

# ==================== 顯示統計摘要 ====================
print("\n📈 統計摘要：")
print(f"總登入人次：{int(pivot_instances['總計'].sum()):,}")
print(f"總登入人數：{int(pivot_users['總計'].max()):,}")
print(f"累計登入人數（最後一個月）：{int(pivot_users['累計'].iloc[-1]):,}")

print("\n各學制總計：")
for level in all_levels:
    print(f"  {level} - 登入人次: {int(pivot_instances[level].sum()):,}, 登入人數: {int(pivot_users[level].max()):,}")

# ==================== 寫入資料庫 ====================
print("\n正在寫入資料庫...")
try:
    from backend.database.service_lab import ServiceLAB
    from backend.database.service_ncu import ServiceNCU
    
    # 準備資料
    batch_date = datetime.now().date()
    stats_data = []
    
    for month in pivot_instances.index:
        for level in all_levels + ['總計']:
            cumulative = int(pivot_users.loc[month, '累計']) if level == '總計' and '累計' in pivot_users.columns else 0
            stats_data.append({
                'month': month,
                'student_level': level,
                'login_instances': int(pivot_instances.loc[month, level]),
                'login_users': int(pivot_users.loc[month, level]),
                'cumulative_users': cumulative
            })
    
    # 寫入 LAB 資料庫
    print("正在連接 LAB 資料庫...")
    db_lab = ServiceLAB()
    db_lab.createTable_login_stats()
    inserted_count_lab = db_lab.insert_login_stats(stats_data, batch_date)
    print(f"✅ 成功寫入 {inserted_count_lab} 筆統計資料到 LAB 資料庫")
    db_lab.cursor.close()
    db_lab.db.close()
    
    # 寫入 NCU 資料庫
    print("正在連接 NCU 資料庫...")
    db_ncu = ServiceNCU()
    db_ncu.createTable_login_stats()
    inserted_count_ncu = db_ncu.insert_login_stats(stats_data, batch_date)
    print(f"✅ 成功寫入 {inserted_count_ncu} 筆統計資料到 NCU 資料庫")
    db_ncu.cursor.close()
    db_ncu.db.close()
    
except Exception as e:
    print(f"⚠️ 寫入資料庫時發生錯誤: {e}")
    import traceback
    traceback.print_exc()

