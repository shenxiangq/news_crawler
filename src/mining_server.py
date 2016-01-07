import logging, logging.config, ConfigParser
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import apscheduler.events
from mongo_connection import get_conn
import lxml.html
import lxml.html.clean

from db_helper import DBHelper
from http_reactor import HttpReactor
from util import valid_a_href, to_unicode

class MiningScheduler(object):

    def __init__(self, mining_server, conf):
        self.logger = logging.getLogger('root')
        self.conn = get_conn(conf.get('db', 'host'), conf.getint('db', 'port'), conf.get('db', 'database'))
        self.mining_server = mining_server
        #self.init_seed()
        self.init_job()

    def init_job(self):
        self.scheduler = BackgroundScheduler()
        executors = {'default': {'type': 'threadpool', 'max_workers': 5},}
        self.scheduler.configure(logger=logging.getLogger('apscheduler'), executors=executors)
        self.scheduler.add_listener(self.err_handle,
                apscheduler.events.EVENT_JOB_ERROR | apscheduler.events.EVENT_JOB_MISSED)

        job_co = self.conn.mining_seed_job
        total = job_co.count()
        interval = 60*60
        time_delta = interval*1.0 / total
        all_job = list(job_co.find())

        for i, job in enumerate(all_job):
            next_run_time = datetime.now() + timedelta(milliseconds=int(time_delta*i*1000), seconds=10)
            self.scheduler.add_job(self.put_to_queue, 'interval',
                args=(job,),
                seconds=interval,
                next_run_time=next_run_time,
                id=str(job.get('_id'))
                )

    def init_seed(self):
        data_list = []
        with open('../data/mining_result_true.txt') as fin:
            for line in fin:
                line = line.strip()
                _, url, block = line.split('\t')
                data_list.append(dict(url=url, block=block))
        self.conn.mining_seed_job.remove({})
        self.conn.mining_seed_job.insert_many(data_list)

    def err_handle(self, ev):
        self.logger.error('apscheduler error:' + str(ev))

    def start(self):
        self.logger.info('scheduler started')
        self.scheduler.start()

    def put_to_queue(self, job):
        url = job.get('url')
        rs = self.conn.mining_seed_task.insert_one(dict(
                job_id=job.get('_id'),
                time=datetime.now()
                ))
        self.mining_server.process_task(url, rs.inserted_id)

class MiningServer(object):
    def __init__(self):
        self.reactor = HttpReactor()
        self.cleaner = lxml.html.clean.Cleaner(style=True, scripts=True, page_structure=False, safe_attrs_only=False)
        self.html_parser = lxml.html.HTMLParser(encoding='utf-8')
        self.logger = logging.getLogger("root")


    def process_body(self, body, url, obj_id):
        body = to_unicode(body)
        body.replace('<?xml version="1.0" encoding="utf-8"?>', '')
        body = self.cleaner.clean_html(body)
        with open('../data/mining_task/'+str(obj_id), 'w') as fout:
            fout.write(body.encode('utf-8'))
        print body[:100].encode('utf-8')

    def process_error(self, failure, url, obj_id):
        print failure.getErrorMessage()
        self.logger.error("download error, url:%s, msg:%s" %
                (url, failure.getTraceback()))

    def process_task(self, url, obj_id):
        url = url.encode('utf-8')
        requestProcess = (lambda x: None, (), {})
        bodyProcess = (self.process_body, (url, obj_id), {})
        errorProcess = (self.process_error, (url, obj_id), {})

        #print "process_task:", url
        self.reactor.download_and_process(url, None, requestProcess, bodyProcess, errorProcess, redirect=True)

    def run(self):
        self.reactor.run()

def main():
    logging.config.fileConfig("../conf/log_mining_crawler.conf")
    config = ConfigParser.ConfigParser()
    config.read('../conf/mining_crawler.conf')
    mining_server = MiningServer()
    scheduler = MiningScheduler(mining_server, config)
    scheduler.start()
    mining_server.run()

if __name__ == '__main__':
    main()
