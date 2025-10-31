import mysql.connector
from backend.database.config import Config
import time


class ServiceNCU:
    def __init__(self):
        # Connect MySQL
        self.db = mysql.connector.connect(
            host=Config.NCU_DB_HOST,
            port=Config.NCU_DB_PORT,
            user=Config.NCU_DB_USERNAME,
            password=Config.NCU_DB_PASSWORD,
            database=Config.NCU_DB_NAME,
            charset='utf8mb4'
        )
        self.cursor = self.db.cursor()

    def table_exist(self, table_name):
        sql = "SHOW TABLES"
        self.cursor.execute(sql)
        tables = [table[0] for table in self.cursor.fetchall()]
        return table_name in tables

    def createTable_clientEvent(self):
        sql = 'CREATE TABLE IF NOT EXISTS client_event' \
              '(client_event_timestamp INT NOT NULL,' \
              ' client_event_time DATETIME NOT NULL,' \
              ' server_sign_token VARCHAR(100) NOT NULL,' \
              ' action_activity VARCHAR(50) NOT NULL,' \
              ' video_info_currentTime INT NOT NULL,' \
              ' encrypt BOOLEAN NULL,' \
              ' actor_id BIGINT NULL,' \
              ' action_id BIGINT NULL,' \
              ' result_id BIGINT NULL,' \
              ' video_info_id BIGINT NULL,' \
              ' filename VARCHAR(20) NOT NULL,' \
              ' CONSTRAINT PK_CLIENT_EVENT PRIMARY KEY (client_event_timestamp, server_sign_token, action_activity, video_info_currentTime));'
        self.cursor.execute(sql)

    def createTable_actor(self):
        sql = 'CREATE TABLE IF NOT EXISTS actor' \
              '(id BIGINT NOT NULL,' \
              ' login_method VARCHAR(10) NOT NULL,' \
              ' uid VARCHAR(10) NOT NULL,' \
              ' session VARCHAR(100) NOT NULL,' \
              ' role VARCHAR(10) NULL,' \
              ' category VARCHAR(5) NULL,' \
              ' grade VARCHAR(5) NULL,' \
              ' city VARCHAR(5) NULL,' \
              ' district VARCHAR(5) NULL,' \
              ' school VARCHAR(50) NULL,' \
              ' tanet_info_id BIGINT NULL,' \
              ' cookie_id BIGINT NULL,' \
              ' ip VARCHAR(20) NULL,' \
              ' filename VARCHAR(255) NULL,' \
              ' CONSTRAINT PK_ACTOR PRIMARY KEY (id), INDEX IDX_ACTOR_FILENAME (filename));'
        self.cursor.execute(sql)

    def createTable_actor_tanet_info(self):
        sql = 'CREATE TABLE IF NOT EXISTS tanet_info' \
              '(tanet_info_id BIGINT NOT NULL,' \
              ' id VARCHAR(20) NOT NULL,' \
              ' userid VARCHAR(10) NOT NULL,' \
              ' sub VARCHAR(255) NOT NULL,' \
              ' timecreated DATETIME NOT NULL,' \
              ' schoolid VARCHAR(20) NULL,' \
              ' grade VARCHAR(20) NULL,' \
              ' identity VARCHAR(10) NULL,' \
              ' seatno VARCHAR(5) NULL,' \
              ' year VARCHAR(5) NULL,' \
              ' semester VARCHAR(5) NULL,' \
              ' classno VARCHAR(10) NULL,' \
              ' filename VARCHAR(255) NULL,' \
              ' CONSTRAINT PK_TANET_INFO PRIMARY KEY (tanet_info_id), INDEX IDX_TANET_INFO_FILENAME (filename));'
        self.cursor.execute(sql)

    def createTable_actor_cookie(self):
        sql = 'CREATE TABLE IF NOT EXISTS cookie' \
              '(id BIGINT NOT NULL,' \
              ' _fbc VARCHAR(255) NULL,' \
              ' _fbp VARCHAR(200) NULL,' \
              ' _ga_E0PD0RQE64 VARCHAR(255) NULL,' \
              ' MoodleSession VARCHAR(100) NULL,' \
              ' _ga VARCHAR(200) NULL,' \
              ' cf_clearance LONGTEXT NULL,' \
              ' filename VARCHAR(255) NULL,' \
              ' CONSTRAINT PK_COOKIE PRIMARY KEY (id), INDEX IDX_COOKIE_FILENAME (filename));'
        self.cursor.execute(sql)

    def createTable_action(self):
        sql = 'CREATE TABLE IF NOT EXISTS action' \
              '(id BIGINT NOT NULL,' \
              ' activity VARCHAR(50) NOT NULL,' \
              ' uri VARCHAR(255) NULL,' \
              ' cm_id VARCHAR(20) NULL,' \
              ' cm_name VARCHAR(50) NULL,' \
              ' categories_id VARCHAR(50) NULL,' \
              ' categories_name VARCHAR(255) NULL,' \
              ' course_id VARCHAR(20) NULL,' \
              ' course_name VARCHAR(255) NULL,' \
              ' section_id VARCHAR(20) NULL,' \
              ' section_name VARCHAR(255) NULL,' \
              ' event_info_id BIGINT NULL,' \
              ' filename VARCHAR(255) NULL,' \
              ' CONSTRAINT PK_ACTION PRIMARY KEY (id), INDEX IDX_ACTION (cm_id), INDEX IDX_ACTION_FILENAME (filename));'
        self.cursor.execute(sql)

    def createTable_action_event_info(self):
        sql = 'CREATE TABLE IF NOT EXISTS event_info' \
              '(id BIGINT NOT NULL,' \
              ' sentences_time FLOAT NULL,' \
              ' sentence VARCHAR(255) NULL,' \
              ' passed BOOLEAN NULL,' \
              ' results VARCHAR(200) NULL,' \
              ' results_score INT(3) NULL,' \
              ' results_errorWords VARCHAR(255) NULL,' \
              ' input_content VARCHAR(255) NULL,' \
              ' sentences BOOLEAN NULL,' \
              ' message LONGTEXT NULL,' \
              ' target LONGTEXT NULL,' \
              ' source VARCHAR(200) NULL,' \
              ' speed VARCHAR(5) NULL,' \
              ' content_recognized LONGTEXT NULL,' \
              ' input LONGTEXT NULL,' \
              ' filename VARCHAR(255) NULL,' \
              ' CONSTRAINT PK_EVENT_INFO PRIMARY KEY (id), INDEX IDX_EVENT_INFO_FILENAME (filename));'
        self.cursor.execute(sql)

    def createTable_action_event_info_sentences(self):
        sql = 'CREATE TABLE IF NOT EXISTS sentences' \
              '(id BIGINT NOT NULL AUTO_INCREMENT,' \
              ' event_info_id BIGINT NOT NULL,' \
              ' original_sentence LONGTEXT NULL,' \
              ' revised_sentence LONGTEXT NULL,' \
              ' feedback LONGTEXT NULL,' \
              ' filename VARCHAR(255) NULL,' \
              ' CONSTRAINT PK_SENTENCES PRIMARY KEY (id), INDEX IDX_SENTENCES_FILENAME (filename));'
        self.cursor.execute(sql)

    def createTable_result(self):
        sql = 'CREATE TABLE IF NOT EXISTS result' \
              '(id BIGINT NOT NULL,' \
              ' score INT NULL,' \
              ' total_time VARCHAR(20) NULL,' \
              ' success VARCHAR(10) NULL,' \
              ' interactionType VARCHAR(100) NULL,' \
              ' correctResponsesPattern VARCHAR(200) NULL,' \
              ' filename VARCHAR(255) NULL,' \
              ' CONSTRAINT PK_RESULT PRIMARY KEY (id), INDEX IDX_RESULT_FILENAME (filename));'
        self.cursor.execute(sql)

    def createTable_video_info(self):
        sql = 'CREATE TABLE IF NOT EXISTS video_info' \
              '(id BIGINT NOT NULL,' \
              ' duration INT NULL,' \
              ' currentSrc VARCHAR(200) NULL,' \
              ' playbackRate FLOAT NULL,' \
              ' currentTime INT NULL,' \
              ' filename VARCHAR(255) NULL,' \
              ' CONSTRAINT PK_VIDEO_INFO PRIMARY KEY (id), INDEX IDX_VIDEO_INFO_FILENAME (filename));'
        self.cursor.execute(sql)

    def checkClientEventNewRecord(self, client_event_time, server_sign_token, action_activity, video_info_currentTime):
        sql = 'SELECT count(*) FROM client_event WHERE client_event_time = %s AND server_sign_token = %s AND action_activity = %s AND video_info_currentTime = %s;'
        var = (client_event_time,
               server_sign_token,
               action_activity,
               video_info_currentTime)
        self.cursor.execute(sql, var)
        result = self.cursor.fetchone()
        return result

    def checkActorNewRecord(self, actorId):
        sql = 'SELECT count(*) FROM actor WHERE id = %s;'
        var = (actorId,)
        self.cursor.execute(sql, var)
        result = self.cursor.fetchone()
        return result

    def checkActionNewRecord(self, actionId):
        sql = 'SELECT count(*) FROM action WHERE id = %s;'
        var = (actionId,)
        self.cursor.execute(sql, var)
        result = self.cursor.fetchone()
        return result

    def checkSentencesNewRecord(self, event_info_id):
        sql = 'SELECT count(*) FROM sentences WHERE event_info_id = %s;'
        var = (event_info_id,)
        self.cursor.execute(sql, var)
        result = self.cursor.fetchone()
        return result

    def checkResultNewRecord(self, resultId):
        sql = 'SELECT count(*) FROM result WHERE id = %s;'
        var = (resultId,)
        self.cursor.execute(sql, var)
        result = self.cursor.fetchone()
        return result

    def checkVideoNewRecord(self, videoId):
        sql = 'SELECT count(*) FROM video_info WHERE id = %s;'
        var = (videoId,)
        self.cursor.execute(sql, var)
        result = self.cursor.fetchone()
        return result

    def getClientEventElementId(self):
        sql = 'SELECT IFNULL(MAX(actor_id), 0) AS actor,' \
              '       IFNULL(MAX(action_id), 0) AS action,' \
              '       IFNULL(MAX(result_id), 0) AS result,' \
              '       IFNULL(MAX(video_info_id), 0) AS video_info' \
              '  FROM client_event;'
        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        return result

    def getActorElementId(self):
        sql = 'SELECT IFNULL(MAX(tanet_info_id), 0) AS tanet_info,' \
              '       IFNULL(MAX(cookie_id), 0) AS cookie' \
              '  FROM actor;'
        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        return result

    def getActionElementId(self):
        sql = 'SELECT IFNULL(MAX(event_info_id), 0) AS event_info' \
              '  FROM action;'
        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        return result

    def get_max_id(self, table_name):
        id_column = {
            'actor': 'id',
            'action': 'id',
            'result': 'id',
            'event_info': 'id',
            'video_info': 'id',
            'tanet_info': 'tanet_info_id',
            'cookie': 'id'
        }[table_name]
        sql = f"SELECT IFNULL(MAX({id_column}), 0) FROM {table_name};"
        self.cursor.execute(sql)
        return self.cursor.fetchone()[0]

    def insertDataToClientEvent(self, client_event):
        sql = 'INSERT IGNORE INTO client_event(client_event_timestamp, client_event_time, server_sign_token, action_activity, video_info_currentTime, encrypt, actor_id, action_id, result_id, video_info_id, filename) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        values = (client_event['client_event_timestamp'],
                  client_event['client_event_time'],
                  client_event['server_sign_token'],
                  client_event['action_activity'],
                  client_event['video_info_currentTime'],
                  client_event['encrypt'],
                  client_event['actor_id'],
                  client_event['action_id'],
                  client_event['result_id'],
                  client_event['video_id'],
                  client_event['filename'])
        self.cursor.execute(sql, values)
        self.db.commit()

    def insertDataToActor(self, actor):
        sql = 'INSERT IGNORE INTO actor(id, login_method, uid, session, role, category, grade, city, district, school, tanet_info_id, cookie_id, ip, filename) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        values = (actor['id'],
                  actor['login_method'],
                  actor['uid'],
                  actor['session'],
                  actor['role'],
                  actor['category'],
                  actor['grade'],
                  actor['city'],
                  actor['district'],
                  actor['school'],
                  actor['tanet_info_id'],
                  actor['cookie_id'],
                  actor['ip'],
                  actor['filename'])
        self.cursor.execute(sql, values)
        self.db.commit()

    def insertDataToTanetInfo(self, tanet_info):
        sql = 'INSERT IGNORE INTO tanet_info(tanet_info_id, id, userid, sub, timecreated, schoolid, grade, identity, seatno, year, semester, classno, filename) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        values = (tanet_info['tanet_info_id'],
                  tanet_info['id'],
                  tanet_info['userid'],
                  tanet_info['sub'],
                  tanet_info['timecreated'],
                  tanet_info['schoolid'],
                  tanet_info['grade'],
                  tanet_info['identity'],
                  tanet_info['seatno'],
                  tanet_info['year'],
                  tanet_info['semester'],
                  tanet_info['classno'],
                  tanet_info['filename'])
        self.cursor.execute(sql, values)
        self.db.commit()

    def insertDataToCookie(self, cookie):
        sql = 'INSERT IGNORE INTO cookie(id, _fbc, _fbp, _ga_E0PD0RQE64, MoodleSession, _ga, cf_clearance, filename) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s);'
        values = (cookie['id'],
                  cookie['_fbc'],
                  cookie['_fbp'],
                  cookie['_ga_E0PD0RQE64'],
                  cookie['MoodleSession'],
                  cookie['_ga'],
                  cookie['cf_clearance'],
                  cookie['filename'])
        self.cursor.execute(sql, values)
        self.db.commit()

    def insertDataToAction(self, action):
        sql = 'INSERT IGNORE INTO action(id, activity, uri, cm_id, cm_name, categories_id, categories_name, course_id, course_name, section_id, section_name, event_info_id, filename) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        values = (action['id'],
                  action['activity'],
                  action['uri'],
                  action['cm_id'],
                  action['cm_name'],
                  action['categories_id'],
                  action['categories_name'],
                  action['course_id'],
                  action['course_name'],
                  action['section_id'],
                  action['section_name'],
                  action['event_info_id'],
                  action['filename'])
        self.cursor.execute(sql, values)
        self.db.commit()

    def insertDataToEventInfo(self, event_info):
        sql = 'INSERT IGNORE INTO event_info(id, sentences_time, sentence, passed, results, results_score, results_errorWords, input_content, sentences, message, target, source, speed, content_recognized, input, filename) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        values = (event_info['id'],
                  event_info['sentences_time'],
                  event_info['sentence'],
                  event_info['passed'],
                  event_info['results'],
                  event_info['results_score'],
                  event_info['results_errorWords'],
                  event_info['input_content'],
                  event_info['sentences'],
                  event_info['message'],
                  event_info['target'],
                  event_info['source'],
                  event_info['speed'],
                  event_info['content_recognized'],
                  event_info['input'],
                  event_info['filename'])
        self.cursor.execute(sql, values)
        self.db.commit()

    def insertDataToSentences(self, sentences):
        sql = 'INSERT IGNORE INTO sentences(event_info_id, original_sentence, revised_sentence, feedback, filename) ' \
              'VALUES (%s, %s, %s, %s, %s);'
        values = (sentences['event_info_id'],
                  sentences['original_sentence'],
                  sentences['revised_sentence'],
                  sentences['feedback'],
                  sentences['filename'])
        self.cursor.execute(sql, values)
        self.db.commit()

    def insertDataToResult(self, result):
        sql = 'INSERT IGNORE INTO result(id, score, total_time, success, interactionType, correctResponsesPattern, filename) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s);'
        values = (result['id'],
                  result['score'],
                  result['total_time'],
                  result['success'],
                  result['interactionType'],
                  result['correctResponsesPattern'],
                  result['filename'])
        self.cursor.execute(sql, values)
        self.db.commit()

    def insertDataToVideoInfo(self, video_info):
        sql = 'INSERT IGNORE INTO video_info(id, duration, currentSrc, playbackRate, currentTime, filename) ' \
              'VALUES (%s, %s, %s, %s, %s, %s);'
        values = (video_info['id'],
                  video_info['duration'],
                  video_info['currentSrc'],
                  video_info['playbackRate'],
                  video_info['currentTime'],
                  video_info['filename'])
        self.cursor.execute(sql, values)
        self.db.commit()

    # 批次插入方法
    def batch_insert_client_events(self, client_events):
        """批次插入client_event資料"""
        if not client_events:
            return
        
        sql = 'INSERT IGNORE INTO client_event(client_event_timestamp, client_event_time, server_sign_token, action_activity, video_info_currentTime, encrypt, actor_id, action_id, result_id, video_info_id, filename) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        
        values_list = []
        for event in client_events:
            values = (event['client_event_timestamp'],
                     event['client_event_time'],
                     event['server_sign_token'],
                     event['action_activity'],
                     event['video_info_currentTime'],
                     event['encrypt'],
                     event['actor_id'],
                     event['action_id'],
                     event['result_id'],
                     event['video_id'],
                     event['filename'])
            values_list.append(values)
        
        self.cursor.executemany(sql, values_list)
        self.db.commit()

    def batch_insert_actors(self, actors):
        """批次插入actor資料"""
        if not actors:
            return
        
        sql = 'INSERT IGNORE INTO actor(id, login_method, uid, session, role, category, grade, city, district, school, tanet_info_id, cookie_id, ip, filename) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        
        values_list = []
        for actor in actors:
            values = (actor['id'],
                     actor['login_method'],
                     actor['uid'],
                     actor['session'],
                     actor['role'],
                     actor['category'],
                     actor['grade'],
                     actor['city'],
                     actor['district'],
                     actor['school'],
                     actor['tanet_info_id'],
                     actor['cookie_id'],
                     actor['ip'],
                     actor['filename'])
            values_list.append(values)
        
        self.cursor.executemany(sql, values_list)
        self.db.commit()

    def batch_insert_tanet_infos(self, tanet_infos):
        """批次插入tanet_info資料"""
        if not tanet_infos:
            return
        
        sql = 'INSERT IGNORE INTO tanet_info(tanet_info_id, id, userid, sub, timecreated, schoolid, grade, identity, seatno, year, semester, classno, filename) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        
        values_list = []
        for tanet_info in tanet_infos:
            values = (tanet_info['tanet_info_id'],
                     tanet_info['id'],
                     tanet_info['userid'],
                     tanet_info['sub'],
                     tanet_info['timecreated'],
                     tanet_info['schoolid'],
                     tanet_info['grade'],
                     tanet_info['identity'],
                     tanet_info['seatno'],
                     tanet_info['year'],
                     tanet_info['semester'],
                     tanet_info['classno'],
                     tanet_info['filename'])
            values_list.append(values)
        
        self.cursor.executemany(sql, values_list)
        self.db.commit()

    def batch_insert_cookies(self, cookies):
        """批次插入cookie資料"""
        if not cookies:
            return
        
        sql = 'INSERT IGNORE INTO cookie(id, _fbc, _fbp, _ga_E0PD0RQE64, MoodleSession, _ga, cf_clearance, filename) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s);'
        
        values_list = []
        for cookie in cookies:
            values = (cookie['id'],
                     cookie['_fbc'],
                     cookie['_fbp'],
                     cookie['_ga_E0PD0RQE64'],
                     cookie['MoodleSession'],
                     cookie['_ga'],
                     cookie['cf_clearance'],
                     cookie['filename'])
            values_list.append(values)
        
        self.cursor.executemany(sql, values_list)
        self.db.commit()

    def batch_insert_actions(self, actions):
        """批次插入action資料"""
        if not actions:
            return
        
        sql = 'INSERT IGNORE INTO action(id, activity, uri, cm_id, cm_name, categories_id, categories_name, course_id, course_name, section_id, section_name, event_info_id, filename) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        
        values_list = []
        for action in actions:
            values = (action['id'],
                     action['activity'],
                     action['uri'],
                     action['cm_id'],
                     action['cm_name'],
                     action['categories_id'],
                     action['categories_name'],
                     action['course_id'],
                     action['course_name'],
                     action['section_id'],
                     action['section_name'],
                     action['event_info_id'],
                     action['filename'])
            values_list.append(values)
        
        self.cursor.executemany(sql, values_list)
        self.db.commit()

    def batch_insert_event_infos(self, event_infos):
        """批次插入event_info資料"""
        if not event_infos:
            return
        
        sql = 'INSERT IGNORE INTO event_info(id, sentences_time, sentence, passed, results, results_score, results_errorWords, input_content, sentences, message, target, source, speed, content_recognized, input, filename) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        
        values_list = []
        for event_info in event_infos:
            values = (event_info['id'],
                     event_info['sentences_time'],
                     event_info['sentence'],
                     event_info['passed'],
                     event_info['results'],
                     event_info['results_score'],
                     event_info['results_errorWords'],
                     event_info['input_content'],
                     event_info['sentences'],
                     event_info['message'],
                     event_info['target'],
                     event_info['source'],
                     event_info['speed'],
                     event_info['content_recognized'],
                     event_info['input'],
                     event_info['filename'])
            values_list.append(values)
        
        self.cursor.executemany(sql, values_list)
        self.db.commit()

    def batch_insert_results(self, results):
        """批次插入result資料"""
        if not results:
            return
        
        sql = 'INSERT IGNORE INTO result(id, score, total_time, success, interactionType, correctResponsesPattern, filename) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s);'
        
        values_list = []
        for result in results:
            values = (result['id'],
                     result['score'],
                     result['total_time'],
                     result['success'],
                     result['interactionType'],
                     result['correctResponsesPattern'],
                     result['filename'])
            values_list.append(values)
        
        self.cursor.executemany(sql, values_list)
        self.db.commit()

    def batch_insert_video_infos(self, video_infos):
        """批次插入video_info資料"""
        if not video_infos:
            return
        
        sql = 'INSERT IGNORE INTO video_info(id, duration, currentSrc, playbackRate, currentTime, filename) ' \
              'VALUES (%s, %s, %s, %s, %s, %s);'
        
        values_list = []
        for video_info in video_infos:
            values = (video_info['id'],
                     video_info['duration'],
                     video_info['currentSrc'],
                     video_info['playbackRate'],
                     video_info['currentTime'],
                     video_info['filename'])
            values_list.append(values)
        
        self.cursor.executemany(sql, values_list)
        self.db.commit()

    def batch_insert_sentences(self, sentences):
        """批次插入sentences資料"""
        if not sentences:
            return
        
        sql = 'INSERT IGNORE INTO sentences(event_info_id, original_sentence, revised_sentence, feedback, filename) ' \
              'VALUES (%s, %s, %s, %s, %s);'
        
        values_list = []
        for sentence in sentences:
            values = (sentence['event_info_id'],
                     sentence['original_sentence'],
                     sentence['revised_sentence'],
                     sentence['feedback'],
                     sentence['filename'])
            values_list.append(values)
        
        self.cursor.executemany(sql, values_list)
        self.db.commit()

    def initTables(self):
        self.createTable_clientEvent()
        self.createTable_actor()
        self.createTable_actor_tanet_info()
        self.createTable_actor_cookie()
        self.createTable_action()
        self.createTable_action_event_info()
        self.createTable_result()
        self.createTable_action_event_info_sentences()
        self.createTable_video_info()

    # 根據 filename 刪除資料的方法
    def delete_client_events_by_filename(self, filename, batch_size=5000):
        """根據 filename 刪除 client_event 資料"""
        total_deleted = 0

        while True:
            sql = 'DELETE FROM client_event WHERE filename = %s LIMIT %s;'
            self.cursor.execute(sql, (filename, batch_size))
            deleted_count = self.cursor.rowcount
            self.db.commit()
            total_deleted += deleted_count
            if deleted_count < batch_size:
                break
        return deleted_count

    def delete_actors_by_filename(self, filename, batch_size=5000):
        """根據 filename 刪除 actor 資料"""
        total_deleted = 0
        
        while True:
            sql = 'DELETE FROM actor WHERE filename = %s LIMIT %s;'
            self.cursor.execute(sql, (filename, batch_size))
            deleted_count = self.cursor.rowcount
            self.db.commit()
            total_deleted += deleted_count
            if deleted_count < batch_size:
                break
        return deleted_count

    def delete_tanet_infos_by_filename(self, filename, batch_size=5000):
        """根據 filename 刪除 tanet_info 資料"""
        total_deleted = 0

        while True:
            sql = 'DELETE FROM tanet_info WHERE filename = %s LIMIT %s;'
            self.cursor.execute(sql, (filename, batch_size))
            deleted_count = self.cursor.rowcount
            self.db.commit()
            total_deleted += deleted_count
            if deleted_count < batch_size:
                break    
        return deleted_count

    def delete_cookies_by_filename(self, filename, batch_size=5000):
        """根據 filename 刪除 cookie 資料"""
        total_deleted = 0

        while True:
            sql = 'DELETE FROM cookie WHERE filename = %s LIMIT %s;'
            self.cursor.execute(sql, (filename, batch_size))
            deleted_count = self.cursor.rowcount
            self.db.commit()
            total_deleted += deleted_count
            if deleted_count < batch_size:
                break
        return deleted_count

    def delete_actions_by_filename(self, filename, batch_size=5000):
        """根據 filename 刪除 action 資料"""
        total_deleted = 0
        
        while True:
            sql = 'DELETE FROM action WHERE filename = %s LIMIT %s;'
            self.cursor.execute(sql, (filename, batch_size))
            deleted_count = self.cursor.rowcount
            self.db.commit()
            total_deleted += deleted_count
            if deleted_count < batch_size:
                break
        return total_deleted

    def delete_event_infos_by_filename(self, filename, batch_size=5000):
        """根據 filename 刪除 event_info 資料"""
        total_deleted = 0

        while True:
            sql = 'DELETE FROM event_info WHERE filename = %s LIMIT %s;'
            self.cursor.execute(sql, (filename, batch_size))
            deleted_count = self.cursor.rowcount
            self.db.commit()
            total_deleted += deleted_count 
            if deleted_count < batch_size:
                break
        return total_deleted

    def delete_sentences_by_filename(self, filename, batch_size=5000):
        """根據 filename 刪除 sentences 資料"""
        total_deleted = 0

        while True:
            sql = 'DELETE FROM sentences WHERE filename = %s LIMIT %s;'
            self.cursor.execute(sql, (filename, batch_size))
            deleted_count = self.cursor.rowcount
            self.db.commit()
            total_deleted += deleted_count
            if deleted_count < batch_size:
                break
        return total_deleted

    def delete_results_by_filename(self, filename, batch_size=5000):
        """根據 filename 刪除 result 資料"""
        total_deleted = 0

        while True:
            sql = 'DELETE FROM result WHERE filename = %s LIMIT %s;'
            self.cursor.execute(sql, (filename,batch_size))
            deleted_count = self.cursor.rowcount
            self.db.commit()
            total_deleted += deleted_count
            if deleted_count < batch_size:
                break
        return total_deleted

    def delete_video_infos_by_filename(self, filename, batch_size=5000):
        """根據 filename 刪除 video_info 資料"""
        total_deleted = 0

        while True:
            sql = 'DELETE FROM video_info WHERE filename = %s LIMIT %s;'
            self.cursor.execute(sql, (filename, batch_size))
            deleted_count = self.cursor.rowcount
            self.db.commit()
            total_deleted += deleted_count
            if deleted_count < batch_size:
                break
        return total_deleted

    def delete_all_data_by_filename(self, filename, batch_size=5000):
        """根據 filename 刪除所有相關 table 的資料"""
        print(f"開始刪除檔案 {filename} 的所有相關資料...")
        
        # 按照外鍵依賴關係的順序刪除
        deleted_counts = {}
        
        # 先刪除沒有外鍵依賴的 table
        deleted_counts['client_event'] = self.delete_client_events_by_filename(filename, batch_size)
        deleted_counts['sentences'] = self.delete_sentences_by_filename(filename, batch_size)
        deleted_counts['result'] = self.delete_results_by_filename(filename, batch_size)
        deleted_counts['video_info'] = self.delete_video_infos_by_filename(filename, batch_size)
        
        # 再刪除有外鍵依賴的 table
        deleted_counts['event_info'] = self.delete_event_infos_by_filename(filename, batch_size)
        deleted_counts['action'] = self.delete_actions_by_filename(filename, batch_size)
        deleted_counts['tanet_info'] = self.delete_tanet_infos_by_filename(filename, batch_size)
        deleted_counts['cookie'] = self.delete_cookies_by_filename(filename, batch_size)
        deleted_counts['actor'] = self.delete_actors_by_filename(filename, batch_size)
        
        total_deleted = sum(deleted_counts.values())
        print(f"檔案 {filename} 的資料刪除完成，共刪除 {total_deleted} 筆資料")
        
        for table, count in deleted_counts.items():
            if count > 0:
                print(f"  - {table}: {count} 筆")
        
        return deleted_counts

    def createTable_ai_course_stats(self):
        """建立 AI 課程統計表"""
        sql = '''CREATE TABLE IF NOT EXISTS ai_course_stats (
            id BIGINT NOT NULL AUTO_INCREMENT,
            ai_course_name VARCHAR(100) NOT NULL,
            student_level VARCHAR(10) NOT NULL,
            month VARCHAR(7) NOT NULL COMMENT '月份：YYYY-MM',
            user_count INT NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            batch_date DATE NOT NULL,
            CONSTRAINT PK_AI_COURSE_STATS PRIMARY KEY (id),
            INDEX IDX_AI_COURSE_STATS_COURSE (ai_course_name),
            INDEX IDX_AI_COURSE_STATS_MONTH (month),
            INDEX IDX_AI_COURSE_STATS_BATCH (batch_date),
            UNIQUE KEY UK_AI_COURSE_STATS (ai_course_name, student_level, month)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;'''
        self.cursor.execute(sql)

    def insert_ai_course_stats(self, stats_data, batch_date):
        """批次插入 AI 課程統計資料"""
        if not stats_data:
            return 0
        
        # 使用 UPSERT：同月資料覆蓋最新結果
        sql = '''INSERT INTO ai_course_stats 
                (ai_course_name, student_level, month, user_count, batch_date) 
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    user_count = VALUES(user_count),
                    batch_date = VALUES(batch_date),
                    created_at = CURRENT_TIMESTAMP'''
        
        values_list = []
        for row in stats_data:
            # 只處理有月份的資料
            month_value = row.get('month')
            if month_value is None or month_value == '':
                continue  # 跳過無月份的資料
            
            values = (
                row.get('ai_course_name'),
                row.get('student_level'),
                month_value,
                row.get('user_count'),
                batch_date
            )
            values_list.append(values)
        
        if values_list:
            self.cursor.executemany(sql, values_list)
            self.db.commit()
        
        return len(values_list)

