import shutil
from datetime import datetime
import glob
import json
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from backend.database.config import Config
from backend.database.service_lab import ServiceLAB
from backend.database.service_ncu import ServiceNCU

# 創建線程鎖，用於保護共享資源
db_lock = threading.Lock()

class Bigdatav2:
    def __init__(self, max_workers=5, batch_size=10000, verbose=True):
        self.system_time = time.strftime("%Y-%m-%d-%H%M%S", time.localtime())
        self.filepath = Config.PROGRESS_FILE_PATH
        self.complete = Config.COMPLETED_FILE_PATH
        # === 新增：錯誤檔案資料夾 ===
        self.error_path = Config.ERROR_FILE_PATH
        self.searchPath = os.path.join(self.filepath, '**', '*.json')
        self.max_workers = max_workers  # 降低預設線程數，減少並發衝突
        self.batch_size = batch_size    # 降低預設批次大小，減少鎖定時間
        self.verbose = verbose  # 是否顯示詳細輸出
        
        # === 新增：確保錯誤資料夾存在 ===
        os.makedirs(self.error_path, exist_ok=True)

    def process_single_log(self, log, filename, db_lab, db_ncu, batch_data, actor_id, action_id, result_id, event_info_id, video_id, tanet_info_id, cookie_id):
        """處理單一日誌記錄的函數，將資料加入批次中"""
        try:
            # === 新增：檢查 log 資料類型 ===
            if not isinstance(log, dict):
                print(f"❌ 錯誤：log 不是 dict 類型，實際類型: {type(log)}, 值: {log}", flush=True)
                return False
            
            # === 新增：檢查必要欄位是否存在且不為 None ===
            required_fields = ['client_event_time', 'server_sign_token', 'action', 'actor', 'result']
            for field in required_fields:
                if field not in log:
                    print(f"❌ 錯誤：log 缺少必要欄位 '{field}', 可用欄位: {list(log.keys())}", flush=True)
                    return False
                if log[field] is None:
                    print(f"❌ 錯誤：log 欄位 '{field}' 為 None，跳過此記錄", flush=True)
                    return False
            
            # === 修正：處理 client_event_time ===
            client_event_time_value = log['client_event_time']
            if isinstance(client_event_time_value, str):
                timestamp = int(float(client_event_time_value))
            else:
                timestamp = int(client_event_time_value)
            dt = datetime.utcfromtimestamp(timestamp)

            # === 新增：資料類型檢查和轉換函數 ===
            def convert_to_string(value):
                """將值轉換為字串，如果是 dict 或 list 則轉為 JSON 字串"""
                if value is None:
                    return None
                elif isinstance(value, (dict, list)):
                    try:
                        return json.dumps(value, ensure_ascii=False)
                    except:
                        return str(value)
                else:
                    return str(value)
            
            def convert_to_bool(value):
                """將值轉換為布林值"""
                if value is None:
                    return None
                elif isinstance(value, bool):
                    return value
                elif isinstance(value, (int, float)):
                    return bool(value)
                elif isinstance(value, str):
                    return value.lower() in ('true', '1', 'yes', 'on')
                else:
                    return bool(value)
            
            def convert_to_float(value):
                """將值轉換為浮點數"""
                if value is None:
                    return None
                elif isinstance(value, (int, float)):
                    return float(value)
                elif isinstance(value, str):
                    try:
                        return float(value)
                    except:
                        return None
                else:
                    return None
            
            def convert_to_int(value):
                """將值轉換為整數"""
                if value is None:
                    return None
                elif isinstance(value, int):
                    return value
                elif isinstance(value, float):
                    return int(value)
                elif isinstance(value, str):
                    try:
                        return int(float(value))
                    except:
                        return None
                else:
                    return None
            
            # 直接使用傳入的 id
            video_info_currentTime = -1
            if 'video_info' in log.keys() and log['video_info'] is not None and isinstance(log['video_info'], dict):
                video_info_currentTime = convert_to_float(log['video_info'].get('currentTime'))

            client_event = dict()
            client_event['client_event_timestamp'] = timestamp
            client_event['client_event_time'] = dt
            client_event['server_sign_token'] = convert_to_string(log['server_sign_token'])
            client_event['action_activity'] = convert_to_string(log['action']['activity'])
            client_event['video_info_currentTime'] = video_info_currentTime
            client_event['encrypt'] = None
            if 'encrypt' in log.keys() and log['encrypt'] is not None:
                client_event['encrypt'] = convert_to_bool(log['encrypt'])
            client_event['actor_id'] = actor_id
            client_event['action_id'] = action_id
            client_event['result_id'] = result_id
            client_event['video_id'] = None
            client_event['filename'] = filename

            # create actor data
            # === 新增：檢查 actor 資料類型 ===
            if not isinstance(log['actor'], dict):
                print(f"❌ 錯誤：actor 不是 dict 類型，實際類型: {type(log['actor'])}, 值: {log['actor']}", flush=True)
                return False
            
            # === 新增：檢查 actor 必要欄位 ===
            actor_required_fields = ['login_method', 'uid', 'session', 'role', 'cookie']
            for field in actor_required_fields:
                if field not in log['actor']:
                    print(f"❌ 錯誤：actor 缺少必要欄位 '{field}', 可用欄位: {list(log['actor'].keys())}", flush=True)
                    return False
                if log['actor'][field] is None:
                    print(f"❌ 錯誤：actor 欄位 '{field}' 為 None，跳過此記錄", flush=True)
                    return False
            
            tanet_info_id_used = None
            if log['actor']['login_method'] == 'TANET' and isinstance(log['actor']['tanet_info'], dict):
                tanet_info_id_used = tanet_info_id
            cookie_id_used = None
            if isinstance(log['actor']['cookie'], dict) and len(log['actor']['cookie']) > 0:
                cookie_id_used = cookie_id

            actor = dict()
            actor['id'] = actor_id
            actor['login_method'] = convert_to_string(log['actor']['login_method'])
            actor['uid'] = convert_to_string(log['actor']['uid'])
            actor['session'] = convert_to_string(log['actor']['session'])
            actor['role'] = convert_to_string(log['actor']['role'])
            actor['category'] = convert_to_string(log['actor']['category'])
            actor['grade'] = convert_to_string(log['actor']['grade'])
            actor['city'] = convert_to_string(log['actor']['city'])
            actor['district'] = convert_to_string(log['actor']['district'])
            actor['school'] = convert_to_string(log['actor']['school'])
            actor['ip'] = convert_to_string(log['actor']['ip'])
            actor['tanet_info_id'] = tanet_info_id_used
            actor['cookie_id'] = cookie_id_used
            actor['filename'] = filename

            tanet_info = None
            if tanet_info_id_used is not None:
                tinfo = log['actor']['tanet_info']
                # === 新增：檢查 tanet_info 資料類型 ===
                if not isinstance(tinfo, dict):
                    print(f"❌ 錯誤：tanet_info 不是 dict 類型，實際類型: {type(tinfo)}, 值: {tinfo}", flush=True)
                    return False
                
                # === 新增：檢查 tanet_info 是否為空 ===
                if not tinfo:
                    print(f"⚠️  警告：tanet_info 為空 dict，跳過 tanet_info 處理", flush=True)
                    tanet_info_id_used = None
                
                tanet_info = dict()
                tanet_info['tanet_info_id'] = tanet_info_id
                tanet_info['id'] = convert_to_string(tinfo['id'])
                tanet_info['userid'] = convert_to_string(tinfo['userid'])
                tanet_info['sub'] = convert_to_string(tinfo['sub'])
                # === 修正：處理 timecreated 欄位 ===
                timecreated_value = tinfo['timecreated']
                if timecreated_value is not None:
                    try:
                        if isinstance(timecreated_value, str):
                            timestamp_tanet = int(float(timecreated_value))
                        else:
                            timestamp_tanet = int(timecreated_value)
                        dt_tanet = datetime.fromtimestamp(timestamp_tanet)
                        tanet_info['timecreated'] = dt_tanet
                    except (ValueError, TypeError) as e:
                        print(f"⚠️  無法解析 timecreated: {timecreated_value}, 錯誤: {e}", flush=True)
                        tanet_info['timecreated'] = None
                else:
                    tanet_info['timecreated'] = None
                tanet_info['schoolid'] = convert_to_string(tinfo['schoolid'])
                tanet_info['grade'] = convert_to_string(tinfo['grade'])
                tanet_info['identity'] = convert_to_string(tinfo['identity'])
                tanet_info['seatno'] = convert_to_string(tinfo['seatno'])
                tanet_info['year'] = convert_to_string(tinfo['year'])
                tanet_info['semester'] = convert_to_string(tinfo['semester'])
                tanet_info['classno'] = convert_to_string(tinfo['classno'])
                tanet_info['filename'] = filename

            cookie = None
            if cookie_id_used is not None:
                c = log['actor']['cookie']
                # === 新增：檢查 cookie 資料類型 ===
                if not isinstance(c, dict):
                    print(f"❌ 錯誤：cookie 不是 dict 類型，實際類型: {type(c)}, 值: {c}", flush=True)
                    return False
                
                # === 新增：檢查 cookie 是否為空 ===
                if not c:
                    print(f"⚠️  警告：cookie 為空 dict，跳過 cookie 處理", flush=True)
                    cookie_id_used = None
                
                cookie = dict()
                cookie['id'] = cookie_id
                # === 恢復原始版本的預設值設定和條件性處理邏輯 ===
                cookie['_fbc'] = None
                if '_fbc' in c.keys():
                    cookie['_fbc'] = convert_to_string(c['_fbc'])

                cookie['_fbp'] = None
                if '_fbp' in c.keys():
                    cookie['_fbp'] = convert_to_string(c['_fbp'])

                cookie['_ga_E0PD0RQE64'] = None
                if '_ga_E0PD0RQE64' in c.keys():
                    cookie['_ga_E0PD0RQE64'] = convert_to_string(c['_ga_E0PD0RQE64'])

                cookie['MoodleSession'] = None
                if 'MoodleSession' in c.keys():
                    cookie['MoodleSession'] = convert_to_string(c['MoodleSession'])

                cookie['_ga'] = None
                if '_ga' in c.keys():
                    cookie['_ga'] = convert_to_string(c['_ga'])

                cookie['cf_clearance'] = None
                if 'cf_clearance' in c.keys():
                    cookie['cf_clearance'] = convert_to_string(c['cf_clearance'])
                
                cookie['filename'] = filename

            # === 新增：檢查 action 資料類型 ===
            if not isinstance(log['action'], dict):
                print(f"❌ 錯誤：action 不是 dict 類型，實際類型: {type(log['action'])}, 值: {log['action']}", flush=True)
                return False
            
            # === 新增：檢查 action 必要欄位 ===
            action_required_fields = ['activity', 'uri']
            for field in action_required_fields:
                if field not in log['action']:
                    print(f"❌ 錯誤：action 缺少必要欄位 '{field}', 可用欄位: {list(log['action'].keys())}", flush=True)
                    return False
                if log['action'][field] is None:
                    print(f"❌ 錯誤：action 欄位 '{field}' 為 None，跳過此記錄", flush=True)
                    return False
            
            action = dict()
            action['id'] = action_id
            action['activity'] = convert_to_string(log['action']['activity'])
            action['uri'] = convert_to_string(log['action']['uri'])
            action['cm_id'] = convert_to_string(log['action']['cm_id'])
            action['cm_name'] = convert_to_string(log['action']['cm_name'])
            action['categories_id'] = convert_to_string(log['action']['categories_id'])
            action['categories_name'] = convert_to_string(log['action']['categories_name'])
            action['course_id'] = convert_to_string(log['action']['course_id'])
            action['course_name'] = convert_to_string(log['action']['course_name'])
            action['section_id'] = convert_to_string(log['action']['section_id'])
            action['section_name'] = convert_to_string(log['action']['section_name'])
            action['event_info_id'] = None
            action['filename'] = filename

            event_info = None
            if (log['action']['event_info'] is not None) and (isinstance(log['action']['event_info'], dict)):
                # === 新增：檢查 event_info 是否為空 ===
                if not log['action']['event_info']:
                    print(f"⚠️  警告：event_info 為空 dict，跳過 event_info 處理", flush=True)
                else:
                    action['event_info_id'] = event_info_id
                    event_info = dict()
                    event_info['id'] = event_info_id
                    event_info['filename'] = filename
                
                    # === 恢復原始版本的預設值設定 ===
                    event_info['sentences_time'] = None
                    event_info['sentence'] = None
                    event_info['passed'] = None
                    event_info['results'] = None
                    event_info['results_score'] = None
                    event_info['results_errorWords'] = None
                    
                    # === 恢復原始版本的條件性處理邏輯 ===
                    if action['activity'] == 'asr':
                        if 'sentences_time' in log['action']['event_info']:
                            event_info['sentences_time'] = convert_to_float(log['action']['event_info']['sentences_time'])
                        if 'sentence' in log['action']['event_info']:
                            event_info['sentence'] = convert_to_string(log['action']['event_info']['sentence'])
                        if 'passed' in log['action']['event_info']:
                            event_info['passed'] = convert_to_bool(log['action']['event_info']['passed'])
                        if 'results' in log['action']['event_info']:
                            event_info['results'] = convert_to_string(log['action']['event_info']['results'])
                        if 'results_score' in log['action']['event_info']:
                            event_info['results_score'] = convert_to_int(log['action']['event_info']['results_score'])
                        if 'results_errorWords' in log['action']['event_info']:
                            event_info['results_errorWords'] = convert_to_string(log['action']['event_info']['results_errorWords'])

                    # === 恢復原始版本的 Writing Assistant 和 TOEIC 處理邏輯 ===
                    event_info['input_content'] = None
                    event_info['sentences'] = 0
                    index1 = action['activity'].find("writingassistant")
                    index2 = action['activity'].find("TOEIC")
                    if index1 == 0 or index2 == 0:
                        if 'input_content' in log['action']['event_info']:
                            event_info['input_content'] = convert_to_string(log['action']['event_info']['input_content'])
                        if 'sentences' in log['action']['event_info'] and isinstance(log['action']['event_info']['sentences'], list):
                            if len(log['action']['event_info']['sentences']) > 0:
                                event_info['sentences'] = 1
                                for s in log['action']['event_info']['sentences']:
                                    # === 新增：檢查 sentences 資料類型 ===
                                    if not isinstance(s, dict):
                                        print(f"❌ 錯誤：sentences 項目不是 dict 類型，實際類型: {type(s)}, 值: {s}", flush=True)
                                        continue
                                    
                                    sentences = dict()
                                    sentences['event_info_id'] = event_info_id
                                    sentences['original_sentence'] = convert_to_string(s.get('original_sentence'))
                                    sentences['revised_sentence'] = convert_to_string(s.get('revised_sentence'))
                                    sentences['feedback'] = convert_to_string(s.get('feedback'))
                                    sentences['filename'] = filename
                                    batch_data['sentences'].append(sentences)
                    
                    # === 恢復原始版本的 AI Chatbot 處理邏輯 ===
                    event_info['message'] = None
                    event_info['target'] = None
                    event_info['source'] = None
                    event_info['speed'] = None
                    event_info['content_recognized'] = None
                    index = action['activity'].find("aichatbot")
                    if index == 0:
                        if 'message' in log['action']['event_info'].keys():
                            event_info['message'] = convert_to_string(log['action']['event_info']['message'])
                        if 'target' in log['action']['event_info'].keys():
                            event_info['target'] = convert_to_string(log['action']['event_info']['target'])
                        if 'source' in log['action']['event_info'].keys():
                            event_info['source'] = convert_to_string(log['action']['event_info']['source'])
                        if 'speed' in log['action']['event_info'].keys():
                            event_info['speed'] = convert_to_string(log['action']['event_info']['speed'])
                        if 'content_recognized' in log['action']['event_info'].keys():
                            event_info['content_recognized'] = convert_to_string(log['action']['event_info']['content_recognized'])

                    # === 恢復原始版本的 TTS 處理邏輯 ===
                    event_info['input'] = None
                    index = action['activity'].find("tts")
                    if index == 0:
                        if 'input' in log['action']['event_info']:
                            event_info['input'] = convert_to_string(log['action']['event_info']['input'])

            # === 新增：檢查 result 資料類型 ===
            if not isinstance(log['result'], dict):
                print(f"❌ 錯誤：result 不是 dict 類型，實際類型: {type(log['result'])}, 值: {log['result']}", flush=True)
                return False
            
            # === 新增：檢查 result 必要欄位 ===
            result_required_fields = ['score', 'total_time', 'success']
            for field in result_required_fields:
                if field not in log['result']:
                    print(f"❌ 錯誤：result 缺少必要欄位 '{field}', 可用欄位: {list(log['result'].keys())}", flush=True)
                    return False
            
            result = dict()
            result['id'] = result_id
            result['score'] = convert_to_int(log['result']['score'])
            result['total_time'] = convert_to_string(log['result']['total_time'])
            result['success'] = convert_to_string(log['result']['success'])
            result['interactionType'] = convert_to_string(log['result']['interactionType'])
            result['correctResponsesPattern'] = convert_to_string(log['result']['correctResponsesPattern'])
            result['filename'] = filename

            video_info = None
            if 'video_info' in log.keys() and log['video_info'] is not None:
                # === 新增：檢查 video_info 資料類型 ===
                if not isinstance(log['video_info'], dict):
                    print(f"❌ 錯誤：video_info 不是 dict 類型，實際類型: {type(log['video_info'])}, 值: {log['video_info']}", flush=True)
                    return False
                
                # === 新增：檢查 video_info 是否為空 ===
                if not log['video_info']:
                    print(f"⚠️  警告：video_info 為空 dict，跳過 video_info 處理", flush=True)
                
                client_event['video_id'] = video_id
                video_info = dict()
                video_info['id'] = video_id
                video_info['duration'] = convert_to_float(log['video_info']['duration'])
                video_info['currentSrc'] = convert_to_string(log['video_info']['currentSrc'])
                video_info['playbackRate'] = convert_to_float(log['video_info']['playbackRate'])
                video_info['currentTime'] = convert_to_float(log['video_info']['currentTime'])
                video_info['filename'] = filename

            # 將所有資料加入批次中
            batch_data['client_events'].append(client_event)
            batch_data['actors'].append(actor)
            if tanet_info is not None:
                batch_data['tanet_infos'].append(tanet_info)
            if cookie is not None:
                batch_data['cookies'].append(cookie)
            batch_data['actions'].append(action)
            if event_info is not None:
                batch_data['event_infos'].append(event_info)
            batch_data['results'].append(result)
            if video_info is not None:
                batch_data['video_infos'].append(video_info)

            return True

        except Exception as e:
            print(f"處理日誌時發生錯誤: {e}", flush=True)
            print(f"錯誤詳情 - log 類型: {type(log)}", flush=True)
            if isinstance(log, dict):
                print(f"log 欄位: {list(log.keys())}", flush=True)
                if 'actor' in log:
                    print(f"actor 類型: {type(log['actor'])}, actor 值: {log['actor']}", flush=True)
                if 'action' in log:
                    print(f"action 類型: {type(log['action'])}, action 值: {log['action']}", flush=True)
                if 'result' in log:
                    print(f"result 類型: {type(log['result'])}, result 值: {log['result']}", flush=True)
            else:
                print(f"log 內容: {log}", flush=True)
            return False

    def batch_insert_data_with_retry(self, db_lab, db_ncu, batch_data, filename, max_retries=3):
        """帶重試機制的批次插入，處理死鎖和鎖等待超時"""
        for attempt in range(max_retries):
            try:
                # 重新開始事務
                db_lab.db.rollback()
                db_ncu.db.rollback()
                
                start = time.time()
                
                # 按順序插入，減少死鎖機率
                # 先插入主表格，再插入關聯表格
                if batch_data['client_events']:
                    db_lab.batch_insert_client_events(batch_data['client_events'])
                    db_ncu.batch_insert_client_events(batch_data['client_events'])
                
                if batch_data['actors']:
                    db_lab.batch_insert_actors(batch_data['actors'])
                    db_ncu.batch_insert_actors(batch_data['actors'])
                
                if batch_data['tanet_infos']:
                    db_lab.batch_insert_tanet_infos(batch_data['tanet_infos'])
                    db_ncu.batch_insert_tanet_infos(batch_data['tanet_infos'])
                
                if batch_data['cookies']:
                    db_lab.batch_insert_cookies(batch_data['cookies'])
                    db_ncu.batch_insert_cookies(batch_data['cookies'])
                
                if batch_data['actions']:
                    db_lab.batch_insert_actions(batch_data['actions'])
                    db_ncu.batch_insert_actions(batch_data['actions'])
                
                if batch_data['event_infos']:
                    db_lab.batch_insert_event_infos(batch_data['event_infos'])
                    db_ncu.batch_insert_event_infos(batch_data['event_infos'])
                
                if batch_data['results']:
                    db_lab.batch_insert_results(batch_data['results'])
                    db_ncu.batch_insert_results(batch_data['results'])
                
                if batch_data['video_infos']:
                    db_lab.batch_insert_video_infos(batch_data['video_infos'])
                    db_ncu.batch_insert_video_infos(batch_data['video_infos'])
                
                if batch_data['sentences']:
                    db_lab.batch_insert_sentences(batch_data['sentences'])
                    db_ncu.batch_insert_sentences(batch_data['sentences'])
                
                # 提交事務
                db_lab.db.commit()
                db_ncu.db.commit()
                
                end = time.time()
                
                # 只在詳細模式下輸出
                if self.verbose:
                    print(f"Thread {threading.current_thread().name} - 檔案 {filename} 批次插入 {len(batch_data['client_events'])} 筆記錄，耗時：{end - start:.4f} 秒", flush=True)
                
                return True
                
            except Exception as e:
                error_msg = str(e)
                
                # 回滾事務
                try:
                    db_lab.db.rollback()
                    db_ncu.db.rollback()
                except:
                    pass
                
                # 檢查是否為死鎖或鎖等待超時
                if "Deadlock" in error_msg or "Lock wait timeout" in error_msg:
                    wait_time = 2 ** attempt  # 指數退避：2, 4, 8 秒
                    print(f"⚠️  死鎖/鎖等待超時，第 {attempt + 1} 次重試，等待 {wait_time} 秒...", flush=True)
                    time.sleep(wait_time)
                    continue
                else:
                    # 其他錯誤，不重試
                    print(f"❌ 批次插入失敗: {error_msg}", flush=True)
                    return False
        
        print(f"❌ 批次插入失敗，已重試 {max_retries} 次", flush=True)
        return False

    def batch_insert_data(self, db_lab, db_ncu, batch_data, filename):
        """批次插入所有資料（保持向後相容）"""
        return self.batch_insert_data_with_retry(db_lab, db_ncu, batch_data, filename)

    def process_file(self, json_file):
        """處理單一JSON檔案的函數"""
        # 每個線程都需要自己的資料庫連接
        db_lab = ServiceLAB()
        db_ncu = ServiceNCU()
        
        # 初始化批次資料結構
        batch_data = {
            'client_events': [],
            'actors': [],
            'tanet_infos': [],
            'cookies': [],
            'actions': [],
            'event_infos': [],
            'results': [],
            'video_infos': [],
            'sentences': []
        }
        
        filename = os.path.basename(json_file)
        
        try:
            # === 新增：檢查檔案是否存在 ===
            if not os.path.exists(json_file):
                print(f"❌ 檔案不存在: {json_file}", flush=True)
                self.move_to_error_folder(json_file, "檔案不存在")
                return False
            
            # === 新增：檢查檔案是否可讀取 ===
            if not os.access(json_file, os.R_OK):
                print(f"❌ 檔案無法讀取: {json_file}", flush=True)
                self.move_to_error_folder(json_file, "檔案無法讀取")
                return False
            
            with open(json_file, mode='r', encoding='utf-8') as file:
                try:
                    log_list = json.load(file)
                except json.JSONDecodeError as e:
                    print(f"❌ JSON 格式錯誤: {json_file} - {e}", flush=True)
                    self.move_to_error_folder(json_file, f"JSON格式錯誤: {e}")
                    return False
                
                print(f"處理檔案: {filename} (共 {len(log_list)} 筆記錄)", flush=True)
                
                # === 新增：檢查 JSON 資料完整性 ===
                if not isinstance(log_list, list):
                    print(f"❌ 錯誤：JSON 檔案 {filename} 的根元素不是 list，實際類型: {type(log_list)}", flush=True)
                    self.move_to_error_folder(json_file, f"JSON根元素不是list，實際類型: {type(log_list)}")
                    return False
                
                # 檢查前幾筆記錄的結構
                for i, log in enumerate(log_list[:5]):  # 只檢查前5筆
                    if not isinstance(log, dict):
                        print(f"❌ 錯誤：JSON 檔案 {filename} 第 {i+1} 筆記錄不是 dict，實際類型: {type(log)}", flush=True)
                        continue
                    
                    # 檢查必要欄位
                    required_fields = ['client_event_time', 'server_sign_token', 'action', 'actor', 'result']
                    missing_fields = [field for field in required_fields if field not in log or log[field] is None]
                    if missing_fields:
                        print(f"⚠️  警告：JSON 檔案 {filename} 第 {i+1} 筆記錄缺少欄位: {missing_fields}", flush=True)

            # 先刪除該檔案的所有舊資料
            print(f"🗑️  刪除檔案 {filename} 的舊資料...", flush=True)
            try:
                print(f"刪除lab舊資料")
                deleted_counts_lab = db_lab.delete_all_data_by_filename(filename, 10000)
                print(f"刪除ncu舊資料")
                deleted_counts_ncu = db_ncu.delete_all_data_by_filename(filename, 10000)
                total_deleted = sum(deleted_counts_lab.values()) + sum(deleted_counts_ncu.values())
                print(f"✅ 已刪除 LAB: {sum(deleted_counts_lab.values())} 筆，NCU: {sum(deleted_counts_ncu.values())} 筆，總計 {total_deleted} 筆舊資料", flush=True)
            except Exception as e:
                print(f"⚠️  刪除舊資料時發生錯誤: {e}", flush=True)
                # 如果是鎖等待超時，嘗試重試
                if "Lock wait timeout" in str(e):
                    print("嘗試重試刪除舊資料...", flush=True)
                    try:
                        time.sleep(5)  # 等待 5 秒
                        deleted_counts_lab = db_lab.delete_all_data_by_filename(filename, 10000)
                        deleted_counts_ncu = db_ncu.delete_all_data_by_filename(filename, 10000)
                        total_deleted = sum(deleted_counts_lab.values()) + sum(deleted_counts_ncu.values())
                        print(f"✅ 重試成功，已刪除 LAB: {sum(deleted_counts_lab.values())} 筆，NCU: {sum(deleted_counts_ncu.values())} 筆，總計 {total_deleted} 筆舊資料", flush=True)
                    except Exception as retry_e:
                        print(f"⚠️  重試刪除舊資料仍然失敗: {retry_e}", flush=True)
                        # 繼續處理，不中斷流程
                else:
                    # 繼續處理，不中斷流程
                    pass

            # === 新增：查詢所有表格的最大 id ===
            max_actor_id_lab = db_lab.get_max_id('actor')
            max_action_id_lab = db_lab.get_max_id('action')
            max_result_id_lab = db_lab.get_max_id('result')
            max_event_info_id_lab = db_lab.get_max_id('event_info')
            max_video_info_id_lab = db_lab.get_max_id('video_info')
            max_tanet_info_id_lab = db_lab.get_max_id('tanet_info')
            max_cookie_id_lab = db_lab.get_max_id('cookie')
            
            max_actor_id_ncu = db_ncu.get_max_id('actor')
            max_action_id_ncu = db_ncu.get_max_id('action')
            max_result_id_ncu = db_ncu.get_max_id('result')
            max_event_info_id_ncu = db_ncu.get_max_id('event_info')
            max_video_info_id_ncu = db_ncu.get_max_id('video_info')
            max_tanet_info_id_ncu = db_ncu.get_max_id('tanet_info')
            max_cookie_id_ncu = db_ncu.get_max_id('cookie')
            
            # 取兩個資料庫中的較大值
            max_actor_id = max(max_actor_id_lab, max_actor_id_ncu)
            max_action_id = max(max_action_id_lab, max_action_id_ncu)
            max_result_id = max(max_result_id_lab, max_result_id_ncu)
            max_event_info_id = max(max_event_info_id_lab, max_event_info_id_ncu)
            max_video_info_id = max(max_video_info_id_lab, max_video_info_id_ncu)
            max_tanet_info_id = max(max_tanet_info_id_lab, max_tanet_info_id_ncu)
            max_cookie_id = max(max_cookie_id_lab, max_cookie_id_ncu)

            success_count = 0
            error_count = 0
            batch_failures = 0  # 新增：批次失敗計數
            total_batches = 0   # 新增：總批次數
            
            for i, log in enumerate(log_list):
                # === 新增：每筆 log 分配新 id ===
                actor_id = max_actor_id + i + 1
                action_id = max_action_id + i + 1
                result_id = max_result_id + i + 1
                event_info_id = max_event_info_id + i + 1
                video_id = max_video_info_id + i + 1
                tanet_info_id = max_tanet_info_id + i + 1
                cookie_id = max_cookie_id + i + 1

                # 傳遞新 id 給 process_single_log
                if self.process_single_log(log, filename, db_lab, db_ncu, batch_data, actor_id, action_id, result_id, event_info_id, video_id, tanet_info_id, cookie_id):
                    success_count += 1
                else:
                    error_count += 1
                
                # 當批次達到指定大小或是最後一筆記錄時，執行批次插入
                if (i + 1) % self.batch_size == 0 or i == len(log_list) - 1:
                    total_batches += 1
                    batch_num = (i // self.batch_size) + 1
                    if self.batch_insert_data_with_retry(db_lab, db_ncu, batch_data, filename):
                        if self.verbose:
                            print(f"✓ 檔案 {filename} 第 {batch_num} 批次插入完成: {len(batch_data['client_events'])} 筆記錄", flush=True)
                    else:
                        print(f"✗ 檔案 {filename} 第 {batch_num} 批次插入失敗", flush=True)
                        batch_failures += 1  # 記錄批次失敗
                    
                    # 清空批次資料
                    batch_data = {
                        'client_events': [],
                        'actors': [],
                        'tanet_infos': [],
                        'cookies': [],
                        'actions': [],
                        'event_infos': [],
                        'results': [],
                        'video_infos': [],
                        'sentences': []
                    }

            print(f"檔案 {filename} 處理完成: {success_count}/{len(log_list)} 筆記錄成功，{error_count} 筆記錄失敗，{batch_failures}/{total_batches} 個批次失敗", flush=True)

            # 只有當沒有批次失敗時才移動檔案
            if batch_failures == 0:
                # 移動檔案到完成資料夾
                folder_name = os.path.basename(os.path.dirname(json_file))
                completed_full_path = os.path.join(self.complete, folder_name)
                os.makedirs(completed_full_path, exist_ok=True)
                shutil.move(json_file, completed_full_path)
                print(f"✓ 檔案 {filename} 已移動到完成資料夾", flush=True)
                return True
            else:
                print(f"⚠️  檔案 {filename} 有 {batch_failures} 個批次失敗，保留在原位置", flush=True)
                return False

        except FileNotFoundError as e:
            print(f"❌ 檔案不存在: {json_file} - {e}", flush=True)
            self.move_to_error_folder(json_file, f"檔案不存在: {e}")
            return False
        except PermissionError as e:
            print(f"❌ 檔案權限錯誤: {json_file} - {e}", flush=True)
            self.move_to_error_folder(json_file, f"檔案權限錯誤: {e}")
            return False
        except UnicodeDecodeError as e:
            print(f"❌ 檔案編碼錯誤: {json_file} - {e}", flush=True)
            self.move_to_error_folder(json_file, f"檔案編碼錯誤: {e}")
            return False
        except Exception as e:
            print(f"處理檔案 {json_file} 時發生錯誤: {e}", flush=True)
            # 記錄錯誤檔案到文字檔
            error_log_file = f"logs/error_files_{self.system_time}.txt"
            os.makedirs("logs", exist_ok=True)
            with open(error_log_file, 'a', encoding='utf-8') as f:
                f.write(f"處理檔案 {json_file} 時發生錯誤: {e}\n")
            
            # === 新增：將一般錯誤檔案也移到錯誤資料夾 ===
            self.move_to_error_folder(json_file, f"處理錯誤: {e}")
            return False
        finally:
            # 關閉資料庫連接
            try:
                db_lab.cursor.close()
                db_lab.db.close()
                db_ncu.cursor.close()
                db_ncu.db.close()
            except:
                pass

    def move_to_error_folder(self, json_file, error_reason):
        """將錯誤檔案移到錯誤資料夾"""
        try:
            # 建立錯誤資料夾的子資料夾（以日期命名）
            error_date_folder = os.path.join(self.error_path, datetime.now().strftime("%Y-%m-%d"))
            os.makedirs(error_date_folder, exist_ok=True)
            
            # 檢查原始檔案是否存在
            if os.path.exists(json_file):
                # 移動檔案到錯誤資料夾
                error_file_path = os.path.join(error_date_folder, os.path.basename(json_file))
                
                # 如果目標檔案已存在，加上時間戳
                if os.path.exists(error_file_path):
                    name, ext = os.path.splitext(os.path.basename(json_file))
                    timestamp = datetime.now().strftime("%H%M%S")
                    error_file_path = os.path.join(error_date_folder, f"{name}_{timestamp}{ext}")
                
                shutil.move(json_file, error_file_path)
                print(f"⚠️  檔案已移到錯誤資料夾: {os.path.basename(json_file)} - 原因: {error_reason}", flush=True)
                
                # 記錄錯誤資訊到文字檔
                error_log_file = f"logs/error_files_{self.system_time}.txt"
                os.makedirs("logs", exist_ok=True)
                with open(error_log_file, 'a', encoding='utf-8') as f:
                    f.write(f"錯誤檔案: {json_file} -> {error_file_path} - 原因: {error_reason}\n")
            else:
                # 檔案不存在，只記錄錯誤資訊
                print(f"⚠️  檔案不存在，無法移動: {json_file}", flush=True)
                
                # 記錄錯誤資訊到文字檔
                error_log_file = f"logs/error_files_{self.system_time}.txt"
                os.makedirs("logs", exist_ok=True)
                with open(error_log_file, 'a', encoding='utf-8') as f:
                    f.write(f"錯誤檔案: {json_file} - 檔案不存在，無法移動 - 原因: {error_reason}\n")
        except Exception as e:
            print(f"❌ 移動錯誤檔案時發生錯誤: {json_file} - {e}", flush=True)
            
            # 記錄錯誤資訊到文字檔
            error_log_file = f"logs/error_files_{self.system_time}.txt"
            os.makedirs("logs", exist_ok=True)
            with open(error_log_file, 'a', encoding='utf-8') as f:
                f.write(f"移動錯誤檔案失敗: {json_file} - 錯誤: {e} - 原因: {error_reason}\n")

    def readLogData(self):
        """單執行緒依序處理所有 JSON 檔案"""
        json_files = glob.glob(self.searchPath, recursive=True)

        if not json_files:
            print("沒有找到要處理的JSON檔案", flush=True)
            return

        print(f"找到 {len(json_files)} 個JSON檔案，依序處理，批次大小: {self.batch_size}", flush=True)

        completed = 0
        for json_file in json_files:
            try:
                result = self.process_file(json_file)
                completed += 1
                if result:
                    print(f"✓ 檔案處理成功: {os.path.basename(json_file)} ({completed}/{len(json_files)})", flush=True)
                else:
                    print(f"✗ 檔案處理失敗: {os.path.basename(json_file)} ({completed}/{len(json_files)})", flush=True)
            except Exception as e:
                print(f"✗ 檔案處理異常: {os.path.basename(json_file)} - {e} ({completed}/{len(json_files)})", flush=True)

        print("所有檔案處理完成", flush=True)
        
