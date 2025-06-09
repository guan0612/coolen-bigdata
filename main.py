import threading
import time

import schedule as schedule
from flask import Flask, jsonify

from backend.database.bigdata import Bigdata
from backend.database.service_lab import ServiceLAB
from backend.database.service_ncu import ServiceNCU

app = Flask(__name__)
db_lab = ServiceLAB()
db_ncu = ServiceNCU()
bigData = Bigdata()


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


@app.route("/importBigData", methods=['POST'])
def importBigData():
    # bigData.fixLogData()
    # bigData.readFixedLogData()
    db_lab.initTables()
    db_ncu.initTables()
    bigData.readLogData()
    return "import BigData to DB"


def job():
    db_lab.initTables()
    db_ncu.initTables()
    bigData.readLogData()


def run_schedule():
    schedule.every().day.at("15:30").do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    # 開一個背景執行緒來跑排程
    scheduler_thread = threading.Thread(target=run_schedule)
    scheduler_thread.daemon = True  # 主程式結束時一併終止
    scheduler_thread.start()

    app.run(host='0.0.0.0', port=5002, debug=True)