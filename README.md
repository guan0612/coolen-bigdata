# 交接文件

## asus大數據分析與匯入資料庫

---

### 目錄結構
```
目錄位於widm server(140.115.54.44)
/home/coolen/
└── app/(coolen-bigdata-backend容器volumn)              # 主要應用程式目錄
    ├── main.py                                         # Flask 主程式 
    ├── requirements.txt                                
    ├── Dockerfile                                      
    ├── backend/                                        
    │   └── database/            
    │       ├── config.py                               # 資料庫配置
    │       ├── service_lab.py                          # LAB 資料庫服務 (widm server backup)
    │       ├── service_ncu.py                          # NCU 資料庫服務 (machine 9) 
    │       └── bigdata_v2.py                           # 大數據處理 v2 
    ├── data/                     
    |   ├── completed/           # 已完成匯入的檔案                       
    │   ├── progressing/         # 等待匯入的檔案
    │   ├── uploads/             # 自江翠獲取的大數據.zip檔
    │   └── error/               # 錯誤檔案 
    ├── logs/                    # 應用程式日誌
    │
    └── coolen_scripts/           
        ├── auto_import.sh            # 自動匯入腳本 
        ├── cleanup_databases.py      # 資料庫清理腳本 
        ├── export_ai_course_stats.py # 統計資料並匯入資料表
        └── logs/                     # 腳本日誌

```

---

## 容器

### 運行中的容器
| 容器名稱 | 映像檔 | 端口映射 | Docker Run 指令 |
|---------|--------|----------|----------------|
| coolen-bigdata-backend | jessica1116/coolen-bigdata-backend:nginx-configured | 5002/tcp | `docker run -d --name coolen-bigdata-backend -v /home/coolen/app:/app -e TZ=Asia/Taipei -e DB_HOST=coolen-mysql -e DB_USER=root -e DB_PASSWORD=ru4cj84coolen -e DB_NAME=coolen_big_data jessica1116/coolen-bigdata-backend:nginx-configured` |
| coolen-mysql | mysql:5.7 | 0.0.0.0:13306->3306/tcp | `docker run -d --name coolen-mysql -p 13306:3306 -e MYSQL_ROOT_PASSWORD=ru4cj84coolen mysql:5.7` |
| nginx-proxy | jc21/nginx-proxy-manager:latest | 0.0.0.0:80-81->80-81/tcp, 0.0.0.0:443->443/tcp | `docker run -d --name nginx-proxy -p 80:80 -p 81:81 -p 443:443 -v /home/widm/nginx/data:/data -v |
| coolen-mysql(on machine 9) |  mysql:8.0 | 0.0.0.0:13306->3306/tcp | docker run -d --name coolen-mysql -p 13306:3306 -e MYSQL_ROOT_PASSWORD=ru4cj84coolen mysql:8.0 |

### 主要容器說明
- **coolen-bigdata-backend**: 主要應用程式容器，運行 Flask API
- **coolen-mysql**: MySQL 資料庫，端口 13306
- **coolen-mysql(on machine 9)**: MySQL 資料庫，端口 13306
- **nginx-proxy**: 反向代理伺服器

---

## 排程

### 當前排程設定
```bash
# 每天 18:00 執行自動匯入
0 18 * * * /home/coolen/app/coolen_scripts/auto_import.sh >> /home/coolen/app/coolen_scripts/logs/auto_import.log 2>&1

# 每月1號凌晨1點執行AI課程統計並將結果匯入資料表（在容器內執行）
0 1 1 * * docker exec coolen-bigdata-backend python3 /app/coolen_scripts/export_ai_course_stats.py >> /home/coolen/app/coolen_scripts/logs/export_ai_course_stats.log 2>&1

# 每月2號凌晨2點執行資料庫清理，刪除3個月前的資料（在容器內執行）
0 2 2 * * docker exec coolen-bigdata-backend python3 /app/coolen_scripts/cleanup_databases.py >> /home/coolen/app/coolen_scripts/logs/cleanup_databases_$(date +\%Y\%m\%d_\%H\%M\%S).log 2>&1

# 每天 6:00 執行自動上傳(此腳本位於江翠機房)
0 6 *** /home/bigData/upload_bigdata.sh >> /home/bigData/upload_status.log 2>&1
```

### 排程說明
1. **自動匯入 (18:00)**: 處理上傳的檔案，匯入資料庫
2. **課程使用人次統計 (每月1號 01:00)**: 依學制、課程，統計目前資料庫每月的使用人次並匯入資料庫
4. **資料庫清理 (每月2號 02:00)**: 刪除 3 個月前的舊資料
5. **自動將大數據傳至widm server (6:00)**(此腳本位於江翠機房)

---

## 主要腳本功能

### 1. auto_import.sh
- **功能**: 自動化資料匯入
- **執行時間**: 每天 18:00
- **主要流程**:
  1. 檢查上傳檔案
  2. 解壓縮檔案
  3. 匯入資料庫
  4. 清理處理完成的檔案

### 3. export_ai_course_stats.py
- **功能**: 統計資料庫並將結果匯入資料庫
- **執行時間**: 每月 1 號 01:00
- **統計方式**: 依照學制、課程ID查詢每月的使用人次
- **QUERY**: 
```bash
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
    CASE 
        WHEN actor.filename REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN 
            SUBSTRING(actor.filename, 1, 7)
        WHEN actor.filename REGEXP '^[0-9]{6}' THEN 
            CONCAT(SUBSTRING(actor.filename, 1, 4), '-', SUBSTRING(actor.filename, 5, 2))
        ELSE SUBSTRING(actor.filename, 1, 7)
    END AS month,
    COUNT(DISTINCT actor.uid) AS user_count
FROM client_event
LEFT JOIN action ON client_event.action_id = action.id
LEFT JOIN actor ON client_event.actor_id = actor.id
WHERE 
    actor.filename IS NOT NULL
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
```

### 2. cleanup_databases.py
- **功能**: 資料庫清理
- **執行時間**: 每月 2 號 02:00
- **清理規則**: 刪除 3 個月前的資料
- **處理表格**: client_event, actor, action, result, event_info, video_info, cookie, tanet_info, sentences

### 3. upload_bigdata.sh(此腳本位於江翠機房)
- **功能**: 傳送大數據資料
- - **執行時間**: 每天 6:00
- **主要流程**:自江翠機房(/home/bigData/bigDataLog/production/date/comLog/date.zip)傳至widm server(home/coolen/app/data/uploads)

---

### 相關資訊
- **外部 API**: https://widm-server.duckdns.org
- **江翠機房連線方式**: widm server(使用anydesk遠端) --> cyberark --> connect by PSM-Putty --> 進到江翠機房 login ncu-bigdatauser

---

