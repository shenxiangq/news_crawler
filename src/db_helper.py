
from mongo_connection import get_conn
import bson
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
        tasks = list(self.task_co.find({'_id': {'$gt': last_task}, 'job_id': self.job_id}).sort(
            '_id', 1).limit(number))
        if tasks:
            self.job_co.update_one({'_id': self.job_id},
                    {'$set': {'last_task': tasks[-1].get('_id')}})
        return tasks

    def init_mining_job(self, urls, continue_run=True):
        if continue_run:
            last_job = self.job_co.find({'url': {'$in': urls}}).sort('_id', -1).limit(1).next()
            self.job_id = last_job.get('_id')
            task_id = last_job.get('last_task')
        else:
            rs = self.task_co.insert_many([{'url': url, 'time': datetime.now(), 'depth': 1} for url in urls])
            task_ids = rs.inserted_ids
            self.job_id = self.job_co.insert({'last_task': task_ids[0], 'time': datetime.now()})
            self.task_co.update_many({'_id': {'$in': task_ids}}, {'$set': {'job_id': self.job_id}})
        return self.task_co.find_one({'_id': task_ids[0]})

    def save_mining_result(self, body, body_size, task, use_file=True):
        if use_file:
            task_id = task['_id']
            filepath = '../data/mining_page/' + str(task_id)
            with open(filepath, 'w') as fout:
                fout.write(body.encode('utf-8', 'ignore'))
        else:
            self.page_co.insert_one(dict(
                task_id=task['_id'],
                body=body,
                body_size=body_size,
                url=task['url'],
                time=datetime.now()
                ))

    def insert_mining_task(self, task, not_exist):
        ts = [dict(url=url, time=datetime.now(), depth=task.get('depth')+1, job_id=self.job_id) for url in not_exist]
        if ts:
            self.task_co.insert_many(ts)


