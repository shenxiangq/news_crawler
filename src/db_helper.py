
from mongo_connection import get_conn
from datetime import datetime

class DBHelper(object):
    def __init__(self, conf):
        host = conf.get('db', 'host')
        port = conf.getint('db', 'port')
        basename = conf.get('db', 'database')
        self.conn = get_conn(host, port, basename)
        self.job_co = self.conn['mining_job']
        self.task_co = self.conn['mining_task']
        self.page_co = self.conn['mining_page']

    def get_mining_task(self, number):
        last_task = self.job_co.find_one({'_id': self.job_id}).get('last_task')
        #print 'job id', self.job_id
        tasks = list(self.task_co.find({'_id': {'$gt': last_task}}).sort(
            '_id', 1).limit(number))
        if tasks:
            self.job_co.update_one({'_id': self.job_id},
                    {'$set': {'last_task': tasks[-1].get('_id')}})
        return tasks

    def init_mining_job(self, url):
        task_id = self.task_co.insert({'url': url, 'time': datetime.now(), 'depth': 1})
        self.job_id = self.job_co.insert({'last_task': task_id, 'time': datetime.now()})
        self.task_co.update_one({'_id': task_id}, {'$set': {'job_id': self.job_id}})
        return self.task_co.find_one({'_id': task_id})

    def save_mining_result(self, body, task):
        self.page_co.insert_one({'task_id': task['_id'], 'body': body, 'time': datetime.now()})

    def insert_mining_task(self, task, not_exist):
        ts = [dict(url=url, time=datetime.now(), depth=task.get('depth')+1, job_id=self.job_id) for url in not_exist]
        if ts:
            self.task_co.insert_many(ts)


