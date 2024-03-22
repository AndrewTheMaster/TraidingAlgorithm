import schedule
import time
import datetime

def job():
    print("Выполнение задачи в", datetime.datetime.now())

schedule.every(15).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
