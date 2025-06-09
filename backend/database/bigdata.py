import shutil
from datetime import datetime
import glob
import json

import os
import time

from backend.database.config import Config
from backend.database.service_lab import ServiceLAB
from backend.database.service_ncu import ServiceNCU

db_lab = ServiceLAB()
db_ncu = ServiceNCU()


class Bigdata:
    def __init__(self):
        self.system_time = time.strftime("%Y-%m-%d-%H%M%S", time.localtime())
        self.filepath = Config.PROGRESS_FILE_PATH
        self.complete = Config.COMPLETED_FILE_PATH
        self.searchPath = self.filepath + '*.json'
        # self.fixedFilePath = Config.FILE_PATH + 'fixed\\'
        # self.searchFixedPath = self.fixedFilePath + '*.json'

    # def fixLogData(self):
    #     if not os.path.exists(self.fixedFilePath):
    #         os.makedirs(self.fixedFilePath)
    #
    #     partFile = dict()
    #     for jsonFile in glob.glob(self.searchPath):
    #         filename = os.path.basename(jsonFile)
    #         basename = filename.split('_')[0]
    #         print(filename)
    #
    #         fix = self.fixedFilePath + filename
    #         if fix in glob.glob(self.searchFixedPath):
    #             continue
    #
    #         if 'of' in filename:
    #             if basename not in partFile.keys():
    #                 partFile[basename] = list()
    #                 partFile[basename].append(filename)
    #             else:
    #                 partFile[basename].append(filename)
    #         else:
    #             # 修復JSON錯誤格式
    #             with open(jsonFile, 'r', encoding='utf-8') as f:
    #                 raw = '[' + f.read() + ']'
    #                 raw = raw.replace('}\n{', '},\n{').replace('}\nnull\n{', '},\n{')
    #                 # 儲存為標準 JSON 格式
    #                 fixedFile = self.fixedFilePath + filename
    #                 with open(fixedFile, 'w', encoding='utf-8') as out:
    #                     data = json.loads(raw)
    #                     json.dump(data, out, ensure_ascii=False, indent=4)
    #     print(partFile)
    #
    #     for basename in partFile:
    #         raw = ''
    #         for filename in partFile[basename]:
    #             index = partFile[basename].index(filename)
    #             file = self.filepath + filename
    #             if index == 0:
    #                 with open(file, 'r', encoding='utf-8') as f:
    #                     raw = '[' + f.read()
    #                     raw = raw.replace('}\n{', '},\n{').replace('}\nnull\n{', '},\n{')
    #             elif index == len(partFile[basename]) - 1:
    #                 with open(file, 'r', encoding='utf-8') as f:
    #                     raw = raw + f.read() + ']'
    #                     raw = raw.replace('}\n{', '},\n{').replace('}\nnull\n{', '},\n{')
    #             else:
    #                 with open(file, 'r', encoding='utf-8') as f:
    #                     raw = raw.replace('}\n{', '},\n{').replace('}\nnull\n{', '},\n{')
    #
    #         # 儲存為標準 JSON 格式
    #         fixedFile = self.fixedFilePath + basename + '.json'
    #         with open(fixedFile, 'w', encoding='utf-8') as out:
    #             data = json.loads(raw)
    #             json.dump(data, out, ensure_ascii=False, indent=4)

    # def readFixedLogData(self):
    #     for fixedFile in glob.glob(self.searchFixedPath):
    #         with open(fixedFile, mode='r', encoding='utf-8') as file:
    #             logList = json.load(file)
    #             filename = os.path.basename(fixedFile)
    #             for log in logList:
    #                 timestamp = int(log['client_event_time'])
    #                 dt = datetime.utcfromtimestamp(timestamp)
    #
    #                 ids = db_lab.getClientEventElementId()
    #                 new = db_lab.checkClientEventNewRecord(dt, log['server_sign_token'])
    #                 if new[0] == 0:
    #                     actor_id = int(ids[0]) + 1
    #                     action_id = int(ids[1]) + 1
    #                     result_id = int(ids[2]) + 1
    #                 else:
    #                     actor_id = int(ids[0])
    #                     action_id = int(ids[1])
    #                     result_id = int(ids[2])
    #
    #                 client_event = dict()
    #                 client_event['client_event_time'] = dt
    #                 client_event['server_sign_token'] = log['server_sign_token']
    #                 client_event['encrypt'] = log['encrypt']
    #                 client_event['actor_id'] = actor_id
    #                 client_event['action_id'] = action_id
    #                 client_event['result_id'] = result_id
    #                 client_event['filename'] = filename
    #                 print(client_event)
    #                 db_lab.insertDataToClientEvent(client_event)
    #                 db_ncu.insertDataToClientEvent(client_event)
    #
    #                 ids = db_lab.getActorElementId()
    #                 tanet_info_id = None
    #                 if log['actor']['login_method'] == 'TANET':
    #                     tanet_info_id = int(ids[0]) + 1
    #                     tanet_info = dict()
    #                     tanet_info['tanet_info_id'] = tanet_info_id
    #                     tanet_info['id'] = log['actor']['tanet_info']['id']
    #                     tanet_info['uid'] = log['actor']['tanet_info']['userid']
    #                     tanet_info['sub'] = log['actor']['tanet_info']['sub']
    #                     timestamp = int(log['actor']['tanet_info']['timecreated'])
    #                     dt = datetime.fromtimestamp(timestamp)
    #                     tanet_info['timecreated'] = dt
    #                     tanet_info['schoolid'] = log['actor']['tanet_info']['schoolid']
    #                     tanet_info['grade'] = log['actor']['tanet_info']['grade']
    #                     tanet_info['identity'] = log['actor']['tanet_info']['identity']
    #                     tanet_info['seatno'] = log['actor']['tanet_info']['seatno']
    #                     tanet_info['year'] = log['actor']['tanet_info']['year']
    #                     tanet_info['semester'] = log['actor']['tanet_info']['semester']
    #                     tanet_info['classno'] = log['actor']['tanet_info']['classno']
    #                     db_lab.insertDataToTanetInfo(tanet_info)
    #                     db_ncu.insertDataToTanetInfo(tanet_info)
    #
    #                 cookie_id = int(ids[1]) + 1
    #                 actor = dict()
    #                 actor['id'] = actor_id
    #                 actor['login_method'] = log['actor']['login_method']
    #                 actor['uid'] = log['actor']['uid']
    #                 actor['session'] = log['actor']['session']
    #                 actor['role'] = log['actor']['role']
    #                 actor['category'] = log['actor']['category']
    #                 actor['grade'] = log['actor']['grade']
    #                 actor['city'] = log['actor']['city']
    #                 actor['district'] = log['actor']['district']
    #                 actor['school'] = log['actor']['school']
    #                 actor['ip'] = log['actor']['ip']
    #                 actor['tanet_info_id'] = tanet_info_id
    #                 actor['cookie_id'] = cookie_id
    #                 db_lab.insertDataToActor(actor)
    #                 db_ncu.insertDataToActor(actor)
    #
    #                 cookie = dict()
    #                 cookie['id'] = cookie_id
    #                 cookie['_fbc'] = None
    #                 if '_fbc' in log['actor']['cookie'].keys():
    #                     cookie['_fbc'] = log['actor']['cookie']['_fbc']
    #
    #                 cookie['_fbp'] = None
    #                 if '_fbp' in log['actor']['cookie'].keys():
    #                     cookie['_fbp'] = log['actor']['cookie']['_fbp']
    #
    #                 cookie['_ga_E0PD0RQE64'] = None
    #                 if '_ga_E0PD0RQE64' in log['actor']['cookie'].keys():
    #                     cookie['_ga_E0PD0RQE64'] = log['actor']['cookie']['_ga_E0PD0RQE64']
    #
    #                 cookie['MoodleSession'] = None
    #                 if 'MoodleSession' in log['actor']['cookie'].keys():
    #                     cookie['MoodleSession'] = log['actor']['cookie']['MoodleSession']
    #
    #                 cookie['_ga'] = None
    #                 if '_ga' in log['actor']['cookie'].keys():
    #                     cookie['_ga'] = log['actor']['cookie']['_ga']
    #                 db_lab.insertDataToCookie(cookie)
    #                 db_ncu.insertDataToCookie(cookie)
    #
    #                 action = dict()
    #                 action['id'] = action_id
    #                 action['activity'] = log['action']['activity']
    #                 action['uri'] = log['action']['uri']
    #                 action['cm_id'] = log['action']['cm_id']
    #                 action['cm_name'] = log['action']['cm_name']
    #                 action['categories_id'] = log['action']['categories_id']
    #                 action['categories_name'] = log['action']['categories_name']
    #                 action['course_id'] = log['action']['course_id']
    #                 action['course_name'] = log['action']['course_name']
    #                 action['section_id'] = log['action']['section_id']
    #                 action['section_name'] = log['action']['section_name']
    #                 action['event_info_id'] = None
    #                 if log['action']['event_info'] != 'null':
    #                     ids = db_lab.getActionElementId()
    #                     event_info_id = int(ids[0]) + 1
    #                     action['event_info_id'] = event_info_id
    #
    #                     event_info = dict()
    #                     event_info['id'] = event_info_id
    #
    #                     event_info['sentences_time'] = None
    #                     event_info['sentence'] = None
    #                     event_info['passed'] = None
    #                     event_info['results'] = None
    #                     event_info['results_score'] = None
    #                     event_info['results_errorWords'] = None
    #                     if log['action']['activity'] == 'asr':
    #                         event_info['sentences_time'] = log['action']['event_info']['sentences_time']
    #                         event_info['sentence'] = log['action']['event_info']['sentence']
    #                         event_info['passed'] = log['action']['event_info']['passed']
    #                         event_info['results'] = log['action']['event_info']['results']
    #                         event_info['results_score'] = log['action']['event_info']['results_score']
    #                         event_info['results_errorWords'] = log['action']['event_info']['results_errorWords']
    #
    #                     event_info['input_content'] = None
    #                     event_info['original_sentence'] = None
    #                     event_info['revised_sentence'] = None
    #                     event_info['feedback'] = None
    #                     if log['action']['activity'] == 'writing':
    #                         event_info['input_content'] = log['action']['event_info']['input_content']
    #                         event_info['original_sentence'] = log['action']['event_info']['original_sentence']
    #                         event_info['revised_sentence'] = log['action']['event_info']['revised_sentence']
    #                         event_info['feedback'] = log['action']['event_info']['feedback']
    #
    #                     event_info['message'] = None
    #                     event_info['target'] = None
    #                     event_info['source'] = None
    #                     event_info['content_recognized'] = None
    #                     if log['action']['activity'] == 'chat_robot':
    #                         event_info['message'] = log['action']['event_info']['message']
    #                         event_info['target'] = log['action']['event_info']['target']
    #                         event_info['source'] = log['action']['event_info']['source']
    #                         event_info['content_recognized'] = log['action']['event_info']['content_recognized']
    #                     db_lab.insertDataToEventInfo(event_info)
    #                     db_ncu.insertDataToEventInfo(event_info)
    #                 db_lab.insertDataToAction(action)
    #                 db_ncu.insertDataToAction(action)

    def readLogData(self):
        for jsonFile in glob.glob(self.searchPath):
            with open(jsonFile, mode='r', encoding='utf-8') as file:
                logList = json.load(file)
                filename = os.path.basename(jsonFile)
                print(filename)
                for log in logList:
                    timestamp = int(log['client_event_time'])
                    dt = datetime.utcfromtimestamp(timestamp)

                    # create client_event data
                    ids = db_ncu.getClientEventElementId()
                    video_info_currentTime = -1
                    if 'video_info' in log.keys():
                        video_info_currentTime = log['video_info']['currentTime']
                    new = db_ncu.checkClientEventNewRecord(dt, log['server_sign_token'], log['action']['activity'], video_info_currentTime)
                    if new[0] == 0:
                        actor_id = int(ids[0]) + 1
                        action_id = int(ids[1]) + 1
                        result_id = int(ids[2]) + 1
                        video_id = int(ids[3]) + 1
                    else:
                        actor_id = int(ids[0])
                        action_id = int(ids[1])
                        result_id = int(ids[2])
                        video_id = int(ids[3])

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
                    ids = db_ncu.getActorElementId()
                    new = db_ncu.checkActorNewRecord(actor_id)
                    if new[0] == 0:
                        tanet_info_id = int(ids[0]) + 1
                        cookie_id = int(ids[1]) + 1
                    else:
                        tanet_info_id = int(ids[0])
                        cookie_id = int(ids[1])

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
                    if (log['action']['event_info'] is not None) and (isinstance(log['action']['event_info'], dict)):
                        ids = db_ncu.getActionElementId()
                        new = db_ncu.checkActionNewRecord(action_id)

                        if new[0] == 0:
                            event_info_id = int(ids[0]) + 1
                        else:
                            event_info_id = int(ids[0])

                        action['event_info_id'] = event_info_id

                        event_info = dict()
                        event_info['id'] = event_info_id

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
                                new = db_ncu.checkSentencesNewRecord(event_info_id)
                                if new[0] == 0:
                                    sentences = dict()
                                    for s in log['action']['event_info']['sentences']:
                                        sentences['event_info_id'] = event_info_id
                                        sentences['original_sentence'] = s['original_sentence']
                                        sentences['revised_sentence'] = s['revised_sentence']
                                        sentences['feedback'] = s['feedback']
                                        db_ncu.insertDataToSentences(sentences)
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

                    if 'video_info' in log.keys():
                        client_event['video_id'] = video_id
                        video_info = dict()
                        video_info['id'] = video_id
                        video_info['duration'] = log['video_info']['duration']
                        video_info['currentSrc'] = log['video_info']['currentSrc']
                        video_info['playbackRate'] = log['video_info']['playbackRate']
                        video_info['currentTime'] = log['video_info']['currentTime']

                    print(client_event)

                    # insert to ncu db
                    start = time.time()
                    db_ncu.insertDataToClientEvent(client_event)
                    db_ncu.insertDataToActor(actor)
                    if actor['tanet_info_id'] is not None:
                        db_ncu.insertDataToTanetInfo(tanet_info)
                    if len(log['actor']['cookie']) > 0:
                        db_ncu.insertDataToCookie(cookie)
                    db_ncu.insertDataToAction(action)
                    if action['event_info_id'] is not None:
                        db_ncu.insertDataToEventInfo(event_info)
                    db_ncu.insertDataToResult(result)
                    if client_event['video_id'] is not None:
                        db_ncu.insertDataToVideoInfo(video_info)
                    end = time.time()
                    print("db_ncu 執行 insert 時間：%f 秒" % (end - start))

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
        db_ncu.cursor.close()
        db_ncu.db.close()