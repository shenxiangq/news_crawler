import threading
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.twisted import TwistedScheduler
import apscheduler.events
import time, datetime
import logging

logging.basicConfig(format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
datefmt='%a, %d %b %Y %H:%M:%S',filename='list.log', filemode='w')

n = 6000
ll = [0 for i in range(n)]

def func(i):
        ll[i] += 1

def err_lis(ev):
    logger = logging.getLogger("")
    logger.error(str(ev))

scheduler = BackgroundScheduler()
#scheduler = TwistedScheduler()
for i in range(n):
    start = datetime.datetime.now() + datetime.timedelta(seconds=i%10)
    scheduler.add_job(func, 'interval', args=(i,), start_date=start, seconds=10)

scheduler.add_listener(err_lis, apscheduler.events.EVENT_JOB_ERROR | apscheduler.events.EVENT_JOB_MISSED)
scheduler.start()
time.sleep(5)
#scheduler.shutdown()
s = 0
for i in ll:
    s+=i
print s

