from seed_task import SeedTask
from queue_service import BlockingQueueService

from mongo_connection import get_conn
from apscheduler.schedulers.background import BackgroundScheduler
import apscheduler.events

from datetime import datetime
import logging, logging.config


class SeedBase(object):
    def __init__(self, conf):
        self.conf = conf

    def get_all_seed(self):
        db = get_conn(self.conf['address'], self.conf['port'],self.conf['db_name'])
        for seed in db.seed.find():
            yield seed

class SeedScheduler(object):
    def __init__(self, handler, conf):
        self.logger = logging.getLogger("root")
        self.handler = handler
        self.init_job(conf)

    def init_job(self, conf):
        base = SeedBase(conf)
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_listener(self.err_handle,
                apscheduler.events.EVENT_JOB_ERROR | apscheduler.events.EVENT_JOB_MISSED)
        for seed in base.get_all_seed():
            print seed
            self.scheduler.add_job(self.handler, 'interval', args=(seed, self.logger),
                    start_date=datetime.now(), seconds=seed['interval'],
                    id=seed['jobid'])

    def err_handle(self, ev):
        self.logger.error(str(ev))

    def start(self):
        self.logger.info('scheduler started')
        self.scheduler.start()

class SeedHandler(object):
    def __init__(self, queue_service):
        self.queue_service = queue_service

    def handle(self, seed, logger):
        url = seed['url']
        seed_task = SeedTask(url)
        success, msg = queue_service.try_enqueue(seed_task)
        if not success:
            logger.error("seed task failed. url=%s, msg=%s" % (url, msg))
        else:
            logger.info("seed task success. url=%s")

def main():
    logging.config.fileConfig("../conf/seed_log.conf")
    conf = dict(address="localhost", port=10010, db_name="news_crawler")
    queue_service = BlockingQueueService(10)
    handler = SeedHandler(queue_service)
    scheduler = SeedScheduler(handler, conf)
    scheduler.start()


if __name__ == '__main__':
    main()
