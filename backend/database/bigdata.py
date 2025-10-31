import shutil
from datetime import datetime
import glob
import json

import os
import time

from backend.database.config import Config
from backend.database.service_lab import ServiceLAB
#from backend.database.service_ncu import ServiceNCU

db_lab = ServiceLAB()
# db_ncu = ServiceNCU()


class Bigdata:
    def __init__(self):
        self.system_time = time.strftime("%Y-%m-%d-%H%M%S", time.localtime())
        # self.filepath = Config.PROGRESS_FILE_PATH
        self.filepath = "/app/data/progressing/2025_06_10"
        self.complete = Config.COMPLETED_FILE_PATH
        self.searchPath = self.filepath + '*.json'

    def readLogData(self):
        for jsonFile in glob.glob(self.searchPath):
            with open(jsonFile, mode='r', encoding='utf-8') as file:
                logList = json.load(file)
                filename = os.path.basename(jsonFile)
                print(f"處理檔案: {filename} (共 {len(logList)} 筆記錄)")
                
                # 先刪除該檔案的所有舊資料
                print(f"🗑️  刪除檔案 {filename} 的舊資料...")
                try:
                    deleted_counts = db_lab.delete_all_data_by_filename(filename)
                    total_deleted = sum(deleted_counts.values())
                    print(f"✅ 已刪除 {total_deleted} 筆舊資料")
                except Exception as e:
                    print(f"⚠️  刪除舊資料時發生錯誤: {e}")
                    # 繼續處理，不中斷流程
                
                for log in logList:
                    timestamp = int(log['client_event_time'])
                    dt = datetime.utcfromtimestamp(timestamp)

                    # create client_event data
                    # ids = db_ncu.getClientEventElementId()
                    ids = db_lab.getClientEventElementId()  # 改用lab的方法
                    video_info_currentTime = -1
                    if 'video_info' in log.keys():
                        video_info_currentTime = log['video_info']['currentTime']
                    
                    # 直接分配新 ID，不需要檢查是否存在
                        actor_id = int(ids[0]) + 1
                        action_id = int(ids[1]) + 1
                        result_id = int(ids[2]) + 1
                        video_id = int(ids[3]) + 1

                    client_event = dict()
                    client_event['client_event_timestamp'] = timestamp
                    client_event['client_event_time'] = dt
                    client_event['server_sign_token'] = log['server_sign_token']
                    client_event['action_activity'] = log['action']['activity']
                    client_event['video_info_currentTime'] = video_info_currentTime
                    client_event['encrypt'] = None
                    if 'encrypt' in log.keys():
                        client_event['encrypt'] = log['encrypt']
                    client_event['actor_id'] = actor_id
                    client_event['action_id'] = action_id
                    client_event['result_id'] = result_id
                    client_event['video_id'] = None
                    client_event['filename'] = filename

                    # create actor data
                    # ids = db_ncu.getActorElementId()
                    ids = db_lab.getActorElementId()  # 改用lab的方法
                    
                    # 直接分配新 ID，不需要檢查是否存在
                    tanet_info_id = int(ids[0]) + 1
                    cookie_id = int(ids[1]) + 1

                    if (log['actor']['login_method'] == 'TANET') and (isinstance(log['actor']['tanet_info'], dict)):
                        tanet_info = dict()
                        tanet_info['tanet_info_id'] = tanet_info_id
                        tanet_info['id'] = log['actor']['tanet_info']['id']
                        tanet_info['userid'] = log['actor']['tanet_info']['userid']
                        tanet_info['sub'] = log['actor']['tanet_info']['sub']
                        timestamp = int(log['actor']['tanet_info']['timecreated'])
                        dt = datetime.fromtimestamp(timestamp)
                        tanet_info['timecreated'] = dt
                        tanet_info['schoolid'] = log['actor']['tanet_info']['schoolid']
                        tanet_info['grade'] = log['actor']['tanet_info']['grade']
                        tanet_info['identity'] = log['actor']['tanet_info']['identity']
                        tanet_info['seatno'] = log['actor']['tanet_info']['seatno']
                        tanet_info['year'] = log['actor']['tanet_info']['year']
                        tanet_info['semester'] = log['actor']['tanet_info']['semester']
                        tanet_info['classno'] = log['actor']['tanet_info']['classno']
                        tanet_info['filename'] = filename
                    else:
                        tanet_info_id = None

                    if len(log['actor']['cookie']) > 0:
                        cookie = dict()
                        cookie['id'] = cookie_id
                        cookie['_fbc'] = None
                        if '_fbc' in log['actor']['cookie'].keys():
                            cookie['_fbc'] = log['actor']['cookie']['_fbc']

                        cookie['_fbp'] = None
                        if '_fbp' in log['actor']['cookie'].keys():
                            cookie['_fbp'] = log['actor']['cookie']['_fbp']

                        cookie['_ga_E0PD0RQE64'] = None
                        if '_ga_E0PD0RQE64' in log['actor']['cookie'].keys():
                            cookie['_ga_E0PD0RQE64'] = log['actor']['cookie']['_ga_E0PD0RQE64']

                        cookie['MoodleSession'] = None
                        if 'MoodleSession' in log['actor']['cookie'].keys():
                            cookie['MoodleSession'] = log['actor']['cookie']['MoodleSession']

                        cookie['_ga'] = None
                        if '_ga' in log['actor']['cookie'].keys():
                            cookie['_ga'] = log['actor']['cookie']['_ga']

                        cookie['cf_clearance'] = None
                        if 'cf_clearance' in log['actor']['cookie'].keys():
                            cookie['cf_clearance'] = log['actor']['cookie']['cf_clearance']
                        
                        cookie['filename'] = filename
                    else:
                        cookie_id = None

                    actor = dict()
                    actor['id'] = actor_id
                    actor['login_method'] = log['actor']['login_method']
                    actor['uid'] = log['actor']['uid']
                    actor['session'] = log['actor']['session']
                    actor['role'] = log['actor']['role']
                    actor['category'] = log['actor']['category']
                    actor['grade'] = log['actor']['grade']
                    actor['city'] = log['actor']['city']
                    actor['district'] = log['actor']['district']
                    actor['school'] = log['actor']['school']
                    actor['ip'] = log['actor']['ip']
                    actor['tanet_info_id'] = tanet_info_id
                    actor['cookie_id'] = cookie_id
                    actor['filename'] = filename

                    action = dict()
                    action['id'] = action_id
                    action['activity'] = log['action']['activity']
                    action['uri'] = log['action']['uri']
                    action['cm_id'] = log['action']['cm_id']
                    action['cm_name'] = log['action']['cm_name']
                    action['categories_id'] = log['action']['categories_id']
                    action['categories_name'] = log['action']['categories_name']
                    action['course_id'] = log['action']['course_id']
                    action['course_name'] = log['action']['course_name']
                    action['section_id'] = log['action']['section_id']
                    action['section_name'] = log['action']['section_name']
                    action['event_info_id'] = None
                    action['filename'] = filename
                    
                    if (log['action']['event_info'] is not None) and (isinstance(log['action']['event_info'], dict)):
                        # ids = db_ncu.getActionElementId()
                        ids = db_lab.getActionElementId()  # 改用lab的方法

                        # 直接分配新 ID，不需要檢查是否存在
                        event_info_id = int(ids[0]) + 1

                        action['event_info_id'] = event_info_id

                        event_info = dict()
                        event_info['id'] = event_info_id
                        event_info['filename'] = filename

                        event_info['sentences_time'] = None
                        event_info['sentence'] = None
                        event_info['passed'] = None
                        event_info['results'] = None
                        event_info['results_score'] = None
                        event_info['results_errorWords'] = None
                        if action['activity'] == 'asr':
                            if 'sentences_time' in log['action']['event_info']:
                                event_info['sentences_time'] = log['action']['event_info']['sentences_time']
                            if 'sentence' in log['action']['event_info']:
                                event_info['sentence'] = log['action']['event_info']['sentence']
                            if 'passed' in log['action']['event_info']:
                                event_info['passed'] = log['action']['event_info']['passed']
                            if 'results' in log['action']['event_info']:
                                event_info['results'] = log['action']['event_info']['results']
                            if 'results_score' in log['action']['event_info']:
                                event_info['results_score'] = log['action']['event_info']['results_score']
                            if 'results_errorWords' in log['action']['event_info']:
                                event_info['results_errorWords'] = log['action']['event_info']['results_errorWords']

                        event_info['input_content'] = None
                        event_info['sentences'] = 0
                        index1 = action['activity'].find("writingassistant")
                        index2 = action['activity'].find("TOEIC")
                        if index1 == 0 or index2 == 0:
                            event_info['input_content'] = log['action']['event_info']['input_content']
                            if len(log['action']['event_info']['sentences']) > 0:
                                event_info['sentences'] = 1
                                for s in log['action']['event_info']['sentences']:
                                    sentences = dict()
                                    sentences['event_info_id'] = event_info_id
                                    sentences['original_sentence'] = s['original_sentence']
                                    sentences['revised_sentence'] = s['revised_sentence']
                                    sentences['feedback'] = s['feedback']
                                    sentences['filename'] = filename
                                    db_lab.insertDataToSentences(sentences)

                        event_info['message'] = None
                        event_info['target'] = None
                        event_info['source'] = None
                        event_info['speed'] = None
                        event_info['content_recognized'] = None
                        index = action['activity'].find("aichatbot")
                        if index == 0:
                            if 'message' in log['action']['event_info'].keys():
                                event_info['message'] = log['action']['event_info']['message']

                            if 'target' in log['action']['event_info'].keys():
                                event_info['target'] = log['action']['event_info']['target']

                            if 'source' in log['action']['event_info'].keys():
                                event_info['source'] = log['action']['event_info']['source']

                            if 'speed' in log['action']['event_info'].keys():
                                event_info['speed'] = log['action']['event_info']['speed']

                            if 'content_recognized' in log['action']['event_info'].keys():
                                event_info['content_recognized'] = log['action']['event_info']['content_recognized']

                        event_info['input'] = None
                        index = action['activity'].find("tts")
                        if index == 0:
                            if 'input' in log['action']['event_info']:
                                event_info['input'] = log['action']['event_info']['input']

                    result = dict()
                    result['id'] = result_id
                    result['score'] = log['result']['score']
                    result['total_time'] = log['result']['total_time']
                    result['success'] = log['result']['success']
                    result['interactionType'] = log['result']['interactionType']
                    result['correctResponsesPattern'] = log['result']['correctResponsesPattern']
                    result['filename'] = filename

                    if 'video_info' in log.keys():
                        client_event['video_id'] = video_id
                        video_info = dict()
                        video_info['id'] = video_id
                        video_info['duration'] = log['video_info']['duration']
                        video_info['currentSrc'] = log['video_info']['currentSrc']
                        video_info['playbackRate'] = log['video_info']['playbackRate']
                        video_info['currentTime'] = log['video_info']['currentTime']
                        video_info['filename'] = filename

                    print(client_event)

                    # insert to lab db
                    start = time.time()
                    db_lab.insertDataToClientEvent(client_event)
                    db_lab.insertDataToActor(actor)
                    if actor['tanet_info_id'] is not None:
                        db_lab.insertDataToTanetInfo(tanet_info)
                    if len(log['actor']['cookie']) > 0:
                        db_lab.insertDataToCookie(cookie)
                    db_lab.insertDataToAction(action)
                    if action['event_info_id'] is not None:
                        db_lab.insertDataToEventInfo(event_info)
                    db_lab.insertDataToResult(result)
                    if client_event['video_id'] is not None:
                        db_lab.insertDataToVideoInfo(video_info)
                    end = time.time()
                    print("db_lab 執行 insert 時間：%f 秒" % (end - start))

            folder_name = os.path.basename(os.path.dirname(jsonFile))
            completed_full_path = os.path.join(self.complete, folder_name)
            os.makedirs(completed_full_path, exist_ok=True)
            shutil.move(jsonFile, completed_full_path)

        db_lab.cursor.close()
        db_lab.db.close()
        # db_ncu.cursor.close()
        # db_ncu.db.close()