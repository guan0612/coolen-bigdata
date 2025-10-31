from datetime import datetime
import os
import zipfile
from werkzeug.utils import secure_filename

from apscheduler.schedulers.background import BackgroundScheduler

from flask import Flask, jsonify, request
import logging
import threading
import queue
import uuid
from datetime import datetime
from flask import jsonify

from backend.database.bigdata import Bigdata
from backend.database.bigdata_v2 import Bigdatav2
from backend.database.service_lab import ServiceLAB
from backend.database.service_ncu import ServiceNCU

app = Flask(__name__)
# 設定最大檔案大小為 1GB
app.config['MAX_CONTENT_LENGTH'] = 1024 * 1024 * 1024
db_lab = ServiceLAB()
db_ncu = ServiceNCU()
bigData = Bigdata()
bigDatav2 = Bigdatav2(max_workers=1, batch_size=10000, verbose=True)

# 設定上傳和解壓縮目錄
UPLOAD_FOLDER = '/app/data/uploads'
EXTRACT_FOLDER = '/app/data/progressing'
ALLOWED_EXTENSIONS = {'zip'}

# 確保目錄存在
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(EXTRACT_FOLDER, exist_ok=True)

# 全局變量，用於追蹤任務狀態
CURRENT_IMPORT_TASK = None
task_queue = queue.Queue()  # 使用 queue.Queue 替代手動鎖管理

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def background_worker():
    global CURRENT_IMPORT_TASK
    
    print("後台工作線程已啟動，開始監聽任務...", flush=True)
    
    while True:
        try:
            # 從隊列中獲取任務（會自動阻塞等待）
            new_task = task_queue.get()
            print(f"收到新任務: {new_task['id']}", flush=True)
            
            CURRENT_IMPORT_TASK = new_task
            task_id = CURRENT_IMPORT_TASK['id']
            start_time = datetime.now()
            
            print(f"[Task {task_id}] Start === {start_time}", flush=True)
            
            # 實際的處理邏輯
            bigDatav2.readLogData()
            
            end_time = datetime.now()
            total_time = end_time - start_time
            
            print(f"[Task {task_id}] Completed: import BigData to DB === {end_time}", flush=True)
            print(f"[Task {task_id}] End === {end_time}", flush=True)
            print(f"[Task {task_id}] Total time: {total_time}", flush=True)
            
            CURRENT_IMPORT_TASK['status'] = 'completed'
            CURRENT_IMPORT_TASK['end_time'] = str(end_time)
            CURRENT_IMPORT_TASK['total_time'] = str(total_time)
            CURRENT_IMPORT_TASK['active'] = False
            
        except Exception as e:
            print(f"[Task {task_id}] Error: {str(e)}", flush=True)
            CURRENT_IMPORT_TASK['status'] = 'error'
            CURRENT_IMPORT_TASK['error'] = str(e)
            CURRENT_IMPORT_TASK['end_time'] = str(datetime.now())
            CURRENT_IMPORT_TASK['active'] = False
        
        finally:
            # 標記任務完成
            task_queue.task_done()
            CURRENT_IMPORT_TASK = None

# 啟動後台工作線程
worker_thread = threading.Thread(target=background_worker)
worker_thread.daemon = True
worker_thread.start()

# 檢查線程是否正常啟動
print(f"後台工作線程已啟動: {worker_thread.is_alive()}", flush=True)

@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route("/api", methods=['GET'])
def hello():
    return jsonify({'text': 'Hello World!'})


@app.route("/initTables", methods=['POST'])
def initTables():
    db_lab.initTables()
    db_ncu.initTables() 
    return "init all tables"


@app.route("/importBigDatav2", methods=['POST'])
def importBigData():
    print(f"收到 importBigDatav2 請求: {datetime.now()}", flush=True)
    
    task_id = str(uuid.uuid4())
    
    # 創建新任務
    new_task = {
        'id': task_id,
        'status': 'queued',
        'start_time': str(datetime.now()),
        'active': True
    }
    
    # 將任務添加到隊列（自動線程安全）
    task_queue.put(new_task)
    print(f"任務 {task_id} 已加入隊列，當前隊列大小: {task_queue.qsize()}", flush=True)
    
    # 立即返回回應
    print(f"立即返回回應給客戶端: {datetime.now()}", flush=True)
    return jsonify({
        'status': 'accepted',
        'task_id': task_id,
        'message': '資料匯入任務已加入隊列，請使用 /checkImportStatus 查詢狀態'
    })

# 檢查匯入狀態的 API
@app.route("/checkImportStatus", methods=['GET'])
def checkImportStatus():
    # 返回當前任務狀態和隊列狀態
    response = {
        'current_task': CURRENT_IMPORT_TASK,
        'queued_tasks': task_queue.qsize()
    }
    return jsonify(response)


@app.route("/unzipAllFiles", methods=['POST'])
def unzipAllFiles():
    """批量解壓縮 uploads 目錄中的所有 ZIP 檔案，但僅處理尚未在 completed 資料夾中存在的檔案"""
    try:
        print(f"開始批量解壓縮 - {datetime.now()}")
        
        # 獲取請求參數
        start_date_str = request.json.get('start_date') if request.is_json else request.form.get('start_date')
        
        # 解析日期參數
        start_date = None
        if start_date_str:
            try:
                # 支援多種日期格式：YYYY-MM-DD, YYYY_MM_DD, YYYYMMDD
                if '-' in start_date_str:
                    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                elif '_' in start_date_str:
                    start_date = datetime.strptime(start_date_str, '%Y_%m_%d')
                else:
                    start_date = datetime.strptime(start_date_str, '%Y%m%d')
                print(f"指定開始日期：{start_date.strftime('%Y-%m-%d')}")
            except ValueError as e:
                return jsonify({'error': f'日期格式錯誤：{start_date_str}，請使用 YYYY-MM-DD 格式'}), 400
        
        # 獲取 uploads 目錄中的所有檔案
        all_files = os.listdir(UPLOAD_FOLDER)
        zip_files = [f for f in all_files if allowed_file(f)]
        
        # 如果指定了開始日期，過濾檔案
        if start_date:
            filtered_files = []
            for filename in zip_files:
                try:
                    # 從檔名中提取日期
                    date_part = filename.split('_')[:3]  
                    if len(date_part) >= 3:
                        file_date_str = f"{date_part[0]}_{date_part[1]}_{date_part[2]}"
                        file_date = datetime.strptime(file_date_str, '%Y_%m_%d')
                        
                        if file_date >= start_date:
                            filtered_files.append(filename)
                        else:
                            print(f"跳過檔案 {filename}（日期：{file_date.strftime('%Y-%m-%d')} < {start_date.strftime('%Y-%m-%d')}）")
                    else:
                        print(f"無法解析檔案日期：{filename}，包含此檔案")
                        filtered_files.append(filename)
                except Exception as e:
                    print(f"解析檔案 {filename} 日期時發生錯誤：{e}，包含此檔案")
                    filtered_files.append(filename)
            
            zip_files = filtered_files
            print(f"過濾後找到 {len(zip_files)} 個符合日期條件的 ZIP 檔案")
        
        if not zip_files:
            return jsonify({
                'message': '沒有找到 ZIP 檔案',
                'total_files': 0,
                'processed_files': 0
            })
        
        print(f"找到 {len(zip_files)} 個 ZIP 檔案")
        
        # 獲取 completed 資料夾中的所有資料夾名稱
        COMPLETED_FOLDER = os.path.join(os.path.dirname(UPLOAD_FOLDER), "completed")
        completed_folders = []
        if os.path.exists(COMPLETED_FOLDER):
            completed_folders = os.listdir(COMPLETED_FOLDER)
        
        print(f"completed 資料夾中有 {len(completed_folders)} 個已處理的資料夾")
        
        # 過濾出尚未處理的 ZIP 檔案
        unprocessed_files = []
        skipped_files = []
        
        for filename in zip_files:
            base_name = os.path.splitext(filename)[0]
            if base_name in completed_folders:
                print(f"跳過已處理的檔案：{filename}")
                skipped_files.append(filename)
            else:
                unprocessed_files.append(filename)
        
        print(f"找到 {len(unprocessed_files)} 個尚未處理的 ZIP 檔案")
        
        if not unprocessed_files:
            return jsonify({
                'message': '所有 ZIP 檔案已經處理過',
                'total_files': len(zip_files),
                'skipped_files': len(skipped_files),
                'processed_files': 0
            })
        
        results = []
        success_count = 0
        error_count = 0
        
        for filename in unprocessed_files:
            try:
                filepath = os.path.join(UPLOAD_FOLDER, filename)
                
                # 解壓縮檔案到 processing 目錄
                extract_path = os.path.join(EXTRACT_FOLDER, os.path.splitext(filename)[0])
                os.makedirs(extract_path, exist_ok=True)
                
                print(f"解壓縮：{filename}")
                with zipfile.ZipFile(filepath, 'r') as zip_ref:
                    zip_ref.extractall(extract_path)
                
                extracted_files = os.listdir(extract_path)
                
                results.append({
                    'filename': filename,
                    'status': 'success',
                    'extracted_files': extracted_files,
                    'extract_path': extract_path,
                    'file_count': len(extracted_files)
                })
                
                success_count += 1
                print(f"✓ {filename} 解壓縮成功，包含 {len(extracted_files)} 個檔案")
                
            except Exception as e:
                error_count += 1
                error_msg = f"解壓縮 {filename} 失敗：{str(e)}"
                print(f"✗ {error_msg}")
                
                results.append({
                    'filename': filename,
                    'status': 'error',
                    'error': str(e)
                })
        
        print(f"批量解壓縮完成：成功 {success_count} 個，失敗 {error_count} 個，跳過 {len(skipped_files)} 個")
        
        response_data = {
            'message': '批量解壓縮完成',
            'total_files': len(zip_files),
            'success_count': success_count,
            'error_count': error_count,
            'skipped_files': len(skipped_files),
            'skipped_list': skipped_files,
            'results': results
        }
        
        if start_date:
            response_data['start_date'] = start_date.strftime('%Y-%m-%d')
            response_data['filtered'] = True
        
        return jsonify(response_data)
        
    except Exception as e:
        print(f"批量解壓縮處理失敗：{str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route("/upload", methods=['POST'])
def upload():
    try:
        print(f"開始處理檔案上傳請求 - {datetime.now()}")
        
        if 'file' not in request.files:
            print("錯誤：沒有上傳檔案")
            return jsonify({'error': '沒有上傳檔案'}), 400
        
        file = request.files['file']
        
        if file.filename == '':
            print("錯誤：沒有選擇檔案")
            return jsonify({'error': '沒有選擇檔案'}), 400
        
        print(f"收到檔案：{file.filename}")
        
        if not allowed_file(file.filename):
            print(f"錯誤：檔案格式不支援 - {file.filename}")
            return jsonify({'error': '只接受 ZIP 檔案'}), 400
        
        # 安全的檔名
        filename = secure_filename(file.filename)
        filename = f"{filename.replace('-', '_')}"
        
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        file.save(filepath)
        
        file_size = os.path.getsize(filepath)
        print(f"檔案已儲存：{filepath}, 大小: {file_size} bytes")
        
        return jsonify({
            'message': '檔案上傳成功',
            'filename': filename,
            'size': file_size,
            'filepath': filepath
        })
        
    except Exception as e:
        print(f"上傳處理失敗：{str(e)}")
        return jsonify({'error': str(e)}), 500


# def job():
#     print(f"開始排程任務 --- {datetime.now()}")
#     try:
#         print(f"Start: db_lab init Tables === {datetime.now()}")
#         db_lab.initTables()
#         print(f"Start: db_ncu init Tables === {datetime.now()}")
#         db_ncu.initTables()
#         print(f"Start: dimport BigData to DB === {datetime.now()}")
#         bigData.readLogData()
#         print("任務成功完成。")
#     except Exception as e:
#         # 捕獲所有可能的異常並記錄
#         print(f"排程任務執行失敗: {e}", exc_info=True)  # exc_info=True 會打印完整的堆疊追蹤
#     print(f"結束排程任務 --- {datetime.now()}")


if __name__ == '__main__':
    # scheduler = BackgroundScheduler(timezone='Asia/Taipei')
    # scheduler.add_job(job, 'cron', hour=22, minute=10)
    # scheduler.start()
    # print("APScheduler 已在背景啟動。" + str(datetime.now()))

    print(f"Flask 應用程式將在 http://0.0.0.0:5002 啟動。")
    app.run(host='0.0.0.0', port=5002, debug=True, use_reloader=False, threaded=True)
