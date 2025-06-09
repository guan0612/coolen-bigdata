import mysql.connector
from backend.database.config import Config


class ServiceLAB:
    def __init__(self):
        # Connect MySQL
        self.db = mysql.connector.connect(
            host=Config.LAB_DB_HOST,
            port=Config.LAB_DB_PORT,
            user=Config.LAB_DB_USERNAME,
            password=Config.LAB_DB_PASSWORD,
            database=Config.LAB_DB_NAME,
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
              ' CONSTRAINT PK_ACTOR PRIMARY KEY (id));'
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
              ' CONSTRAINT PK_TANET_INFO PRIMARY KEY (tanet_info_id));'
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
              ' CONSTRAINT PK_COOKIE PRIMARY KEY (id));'
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
              ' CONSTRAINT PK_ACTION PRIMARY KEY (id), INDEX IDX_ACTION (cm_id));'
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
              ' CONSTRAINT PK_EVENT_INFO PRIMARY KEY (id));'
        self.cursor.execute(sql)

    def createTable_action_event_info_sentences(self):
        sql = 'CREATE TABLE IF NOT EXISTS sentences' \
              '(id BIGINT NOT NULL AUTO_INCREMENT,' \
              ' event_info_id BIGINT NOT NULL,' \
              ' original_sentence LONGTEXT NULL,' \
              ' revised_sentence LONGTEXT NULL,' \
              ' feedback LONGTEXT NULL,' \
              ' CONSTRAINT PK_SENTENCES PRIMARY KEY (id));'
        self.cursor.execute(sql)

    def createTable_result(self):
        sql = 'CREATE TABLE IF NOT EXISTS result' \
              '(id BIGINT NOT NULL,' \
              ' score INT NULL,' \
              ' total_time VARCHAR(20) NULL,' \
              ' success VARCHAR(10) NULL,' \
              ' interactionType VARCHAR(100) NULL,' \
              ' correctResponsesPattern VARCHAR(200) NULL,' \
              ' CONSTRAINT PK_RESULT PRIMARY KEY (id));'
        self.cursor.execute(sql)

    def createTable_video_info(self):
        sql = 'CREATE TABLE IF NOT EXISTS video_info' \
              '(id BIGINT NOT NULL,' \
              ' duration INT NULL,' \
              ' currentSrc VARCHAR(200) NULL,' \
              ' playbackRate FLOAT NULL,' \
              ' currentTime INT NULL,' \
              ' CONSTRAINT PK_VIDEO_INFO PRIMARY KEY (id));'
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
        sql = 'INSERT IGNORE INTO actor(id, login_method, uid, session, role, category, grade, city, district, school, tanet_info_id, cookie_id, ip) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
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
                  actor['ip'])
        self.cursor.execute(sql, values)
        self.db.commit()

    def insertDataToTanetInfo(self, tanet_info):
        sql = 'INSERT IGNORE INTO tanet_info(tanet_info_id, id, userid, sub, timecreated, schoolid, grade, identity, seatno, year, semester, classno) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
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
                  tanet_info['classno'])
        self.cursor.execute(sql, values)
        self.db.commit()

    def insertDataToCookie(self, cookie):
        sql = 'INSERT IGNORE INTO cookie(id, _fbc, _fbp, _ga_E0PD0RQE64, MoodleSession, _ga, cf_clearance) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s);'
        values = (cookie['id'],
                  cookie['_fbc'],
                  cookie['_fbp'],
                  cookie['_ga_E0PD0RQE64'],
                  cookie['MoodleSession'],
                  cookie['_ga'],
                  cookie['cf_clearance'])
        self.cursor.execute(sql, values)
        self.db.commit()

    def insertDataToAction(self, action):
        sql = 'INSERT IGNORE INTO action(id, activity, uri, cm_id, cm_name, categories_id, categories_name, course_id, course_name, section_id, section_name, event_info_id) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
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
                  action['event_info_id'])
        self.cursor.execute(sql, values)
        self.db.commit()

    def insertDataToEventInfo(self, event_info):
        sql = 'INSERT IGNORE INTO event_info(id, sentences_time, sentence, passed, results, results_score, results_errorWords, input_content, sentences, message, target, source, speed, content_recognized, input) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
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
                  event_info['input'])
        self.cursor.execute(sql, values)
        self.db.commit()

    def insertDataToSentences(self, sentences):
        sql = 'INSERT IGNORE INTO sentences(event_info_id, original_sentence, revised_sentence, feedback) ' \
              'VALUES (%s, %s, %s, %s);'
        values = (sentences['event_info_id'],
                  sentences['original_sentence'],
                  sentences['revised_sentence'],
                  sentences['feedback'])
        self.cursor.execute(sql, values)
        self.db.commit()

    def insertDataToResult(self, result):
        sql = 'INSERT IGNORE INTO result(id, score, total_time, success, interactionType, correctResponsesPattern) ' \
              'VALUES (%s, %s, %s, %s, %s, %s);'
        values = (result['id'],
                  result['score'],
                  result['total_time'],
                  result['success'],
                  result['interactionType'],
                  result['correctResponsesPattern'])
        self.cursor.execute(sql, values)
        self.db.commit()

    def insertDataToVideoInfo(self, video_info):
        sql = 'INSERT IGNORE INTO video_info(id, duration, currentSrc, playbackRate, currentTime) ' \
              'VALUES (%s, %s, %s, %s, %s);'
        values = (video_info['id'],
                  video_info['duration'],
                  video_info['currentSrc'],
                  video_info['playbackRate'],
                  video_info['currentTime'])
        self.cursor.execute(sql, values)
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
