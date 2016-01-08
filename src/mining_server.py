import logging, logging.config, ConfigParser, gzip, re
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import apscheduler.events
from mongo_connection import get_conn
import lxml.html
from lxml.html import HTMLParser, document_fromstring, fromstring
import lxml.html.clean

from db_helper import DBHelper
from http_reactor import HttpReactor
from util import valid_a_elements, to_unicode

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
                id=str(job.get('_id')),
                misfire_grace_time=10,
                )

    def init_seed(self):
        data_list = []
        with open('../data/url_filter.txt') as fin:
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
        with open('../data/mining_task/'+str(obj_id), 'wb') as fout:
            g = gzip.GzipFile(mode='wb', fileobj=fout)
            try:
                g.write(body.encode('utf-8'))
            finally:
                g.close()
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

class SeedAnalyzer(object):

    def __init__(self, conf):
        self.conn = get_conn(conf.get('db', 'host'), conf.getint('db', 'port'), conf.get('db', 'database'))
        self.parser = HTMLParser(encoding='utf-8', remove_comments=True, remove_blank_text=True)

    def frequency_analyze(self, job):
        job_id = job.get('_id')
        url = job.get('url')
        pages = self.read_all_task(job_id)
        trees = [document_fromstring(page, parser=self.parser) for page in pages]
        block = job.get('block')
        paths = re.findall("'(.+?)'", block)
        paths = list(set(paths))
        link_trees = [LinkTree(tree, url, paths) for tree in trees]

        #filter non-change block
        to_remove = set()

        #import pdb; pdb.set_trace()
        for i, path in enumerate(paths):
            blocks = [link_tree.blocks[i] for link_tree in link_trees]
            titles = [link_tree.titles[i] for link_tree in link_trees]
            update_rate = self.update_rate(blocks)
            if update_rate > 0.0:
                for title in titles:
                    print '@@'.join(title)
                print update_rate

        [link_tree.filter_index(to_remove) for link_tree in link_trees]

    def update_rate(self, blocks):
        max_block = max(blocks, key=len)
        max_length = len(max_block)
        period = len(blocks)
        merged = []
        for block in blocks:
            merged.extend(block)
        total = len(merged)
        non_dup = set(merged)
        #min_ratio = max_length*1.0/total

        update_num = len(non_dup) - max_length
        update_per_period = update_num*1.0 / period
        return update_per_period

    def analyze(self):
        jobs = list(self.conn.mining_seed_job.find().sort('_id',1).skip(208).limit(2))
        for job in jobs:
            self.frequency_analyze(job)

    def read_all_task(self, job_id):
        tasks = self.conn.mining_seed_task.find({'job_id': job_id})
        pages = []
        for task in tasks:
            _id = task.get('_id')
            try:
                with gzip.open('../data/mining_task/'+str(_id), 'rb') as f:
                    pages.append(f.read())
            except IOError:
                pass
        return pages

class LinkTree(object):
    def __init__(self, tree, url, paths):
        self.tree = tree
        self.paths = paths
        self.blocks = []
        self.titles = []
        for path in paths:
            a_elements = tree.xpath(path)
            al = valid_a_elements(a_elements)
            hrefs = [a.get('href') for a in al]
            text = [a.text.strip() for a in al if a.text]
            hrefs = list(set(hrefs))
            self.blocks.append(hrefs)
            self.titles.append(text)

    def filter_index(self, to_remove):
        self.paths = [path for i, path in enumerate(self.paths) if i not in to_remove]
        self.blocks = [block for i, block in enumerate(self.blocks) if i not in to_remove]


def main():
    logging.config.fileConfig("../conf/log_mining_crawler.conf")
    config = ConfigParser.ConfigParser()
    config.read('../conf/mining_crawler.conf')
    mining_server = MiningServer()
    scheduler = MiningScheduler(mining_server, config)
    scheduler.start()
    mining_server.run()

def analyze():
    config = ConfigParser.ConfigParser()
    config.read('../conf/mining_crawler.conf')
    analyzer = SeedAnalyzer(config)
    analyzer.analyze()

if __name__ == '__main__':
    #main()
    analyze()
