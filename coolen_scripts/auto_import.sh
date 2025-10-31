#!/bin/bash

# 自動化資料匯入腳本
# 功能：定時解壓縮 uploads 中的檔案，然後匯入資料庫

# 設定變數
SERVER_URL="https://widm-server.duckdns.org"
LOG_FILE="/home/coolen/app/coolen_scripts/logs/auto_import.log"

# 注意：這些路徑是 Docker 容器內的路徑，腳本無法直接訪問
# 我們只能通過 API 來檢查檔案狀態
UPLOAD_DIR="/app/data/uploads"
PROGRESS_DIR="/app/data/progressing"

# 創建日誌目錄
mkdir -p /home/coolen/app/coolen_scripts/logs

# 記錄日誌函數
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# 檢查是否有檔案需要解壓縮（通過 API）
check_upload_files() {
    # 嘗試調用解壓縮 API 來檢查檔案
    local response=$(curl -s -X POST "$SERVER_URL/unzipAllFiles" \
        -H "Content-Type: application/json" \
        -d '{}')
    
    # 從回應中提取檔案數量
    local success_count=$(echo "$response" | grep -o '"success_count":[0-9]*' | cut -d':' -f2)
    
    if [ -n "$success_count" ] && [ "$success_count" -gt 0 ]; then
        echo "$success_count"
    else
        echo "0"
    fi
}

# 檢查是否有檔案需要匯入（通過 API）
check_progress_files() {
    # 先檢查解壓縮 API 是否有檔案處理成功
    local unzip_response=$(curl -s -X POST "$SERVER_URL/unzipAllFiles" \
        -H "Content-Type: application/json" \
        -d '{}')
    
    local success_count=$(echo "$unzip_response" | grep -o '"success_count":[0-9]*' | cut -d':' -f2)
    
    if [ -n "$success_count" ] && [ "$success_count" -gt 0 ]; then
        echo "$success_count"  # 有檔案需要處理
    else
        echo "0"  # 沒有檔案需要處理
    fi
}

# 解壓縮檔案
unzip_files() {
    log_message "開始檢查並解壓縮檔案..."
    
    # 直接調用解壓縮 API
    local response=$(curl -s -X POST "$SERVER_URL/unzipAllFiles" \
        -H "Content-Type: application/json" \
        -d '{}')
    
    if [ $? -eq 0 ]; then
        # 使用 Python 解析 JSON 回應
        local success_count=$(echo "$response" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('success_count', 0))")
        local total_files=$(echo "$response" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('total_files', 0))")
        local skipped_files=$(echo "$response" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('skipped_files', 0))")
        
        log_message "解壓縮 API 調用成功"
        log_message "總檔案數: $total_files, 成功處理: $success_count, 跳過: $skipped_files"
        
        if [ "$success_count" -gt 0 ]; then
            log_message "成功解壓縮 $success_count 個檔案"
        else
            log_message "沒有新的檔案需要解壓縮"
        fi
    else
        log_message "解壓縮 API 調用失敗"
        return 1
    fi
}

# 匯入資料
import_data() {
    log_message "開始匯入資料..."
    
    # 直接調用匯入 API
    local response=$(curl -s -X POST "$SERVER_URL/importBigDatav2" \
        -H "Content-Type: application/json" \
        -d '{}')
    
    if [ $? -eq 0 ]; then
        # 使用 Python 解析 JSON 回應
        local status=$(echo "$response" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('status', ''))")
        local task_id=$(echo "$response" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('task_id', ''))")
        local message=$(echo "$response" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('message', ''))")
        
        log_message "匯入 API 調用成功"
        log_message "狀態: $status, 訊息: $message"
        
        if [ "$status" = "accepted" ] && [ -n "$task_id" ] && [ "$task_id" != "None" ]; then
            log_message "任務 ID: $task_id"
            
            # 監控任務狀態
            monitor_task "$task_id"
        else
            log_message "沒有資料需要匯入或任務未被接受"
        fi
    else
        log_message "匯入 API 調用失敗"
        return 1
    fi
}

# 監控任務狀態
monitor_task() {
    local task_id="$1"
    local max_wait=3600  # 最大等待 1 小時
    local wait_time=0
    local check_interval=30  # 每 30 秒檢查一次
    
    log_message "開始監控任務 $task_id 的狀態..."
    
    while [ $wait_time -lt $max_wait ]; do
        local status_response=$(curl -s "$SERVER_URL/checkImportStatus")
        local status=$(echo "$status_response" | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
        
        case "$status" in
            "completed")
                log_message "任務 $task_id 已完成"
                return 0
                ;;
            "error")
                local error=$(echo "$status_response" | grep -o '"error":"[^"]*"' | cut -d'"' -f4)
                log_message "任務 $task_id 失敗: $error"
                return 1
                ;;
            "running"|"queued")
                log_message "任務 $task_id 狀態: $status，繼續等待..."
                sleep $check_interval
                wait_time=$((wait_time + check_interval))
                ;;
            *)
                log_message "未知任務狀態: $status"
                sleep $check_interval
                wait_time=$((wait_time + check_interval))
                ;;
        esac
    done
    
    log_message "任務 $task_id 監控超時"
    return 1
}

# 主執行函數
main() {
    log_message "=== 自動化資料匯入腳本開始執行 ==="
    
    # 步驟 1：解壓縮檔案
    log_message "步驟 1：檢查並解壓縮檔案"
    unzip_files
    
    # 等待一段時間讓解壓縮完成
    sleep 10
    
    # 步驟 2：匯入資料
    log_message "步驟 2：檢查並匯入資料"
    import_data
    
    log_message "=== 自動化資料匯入腳本執行完成 ==="
    log_message ""
}

# 如果直接執行腳本
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    main "$@"
fi
