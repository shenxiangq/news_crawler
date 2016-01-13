#!/usr/bin/env python
# coding=gbk

import logging, logging.config, signal, sys, time, threading, Queue, ConfigParser
from pprint import pformat
from datetime import datetime
import lxml.html

from http_reactor import HttpReactor
from http_threadpool import HttpThreadpool
from queue_service import BlockingQueueService
from item import SeedTask, MinerTask
from hub_extractor import HubExtractor
from article_server import ArticleServer
from db_helper import DBHelper
from url_dedup import URLDedup
from util import valid_a_href, to_unicode


class MinerServer(object):

    def __init__(self, reactor, pool, init_url, conf, use_pool=False):
        self.logger = logging.getLogger("")
        self.reactor = reactor
        self.pool = pool
        self._parse_conf(conf)
        self.db_helper = DBHelper(conf)
        self.url_dedup = URLDedup(conf)
        self.cleaner = lxml.html.clean.Cleaner(style=True, scripts=True, page_structure=False, safe_attrs_only=False)
        self.html_parser = lxml.html.HTMLParser(encoding='utf-8')
        self.init_url = init_url
        self.use_pool = use_pool

    def _parse_conf(self, conf):
        self.get_delay = conf.getfloat('basic', 'delay')
        self.task_number = conf.getint('basic', 'number')
        self.maxdepth = conf.getint('basic', 'maxdepth')

    def process_request(self, response, task):
        url = task.get('url')
        return
        self.logger.info("http response, url:%s, code:%s, phrase:%s, headers:%s" %
                (url, response.code, response.phrase,
                pformat(list(response.headers.getAllRawHeaders()))))

    def process_body(self, body, task):
        url = task.get('url')
        #print url, body[:100][:1000]
        body_size = len(body)
        body = to_unicode(body)
        body.replace('<?xml version="1.0" encoding="utf-8"?>', '')
        body = self.cleaner.clean_html(body)
        self.logger.info("page body, url:%s, body:%s" % (url, body[:100]))
        self.db_helper.save_mining_result(body, body_size, task)
        if task.get('depth') <= self.maxdepth:
            tree = lxml.html.document_fromstring(body)
            a_elements = tree.xpath('//a')
            #import pdb;pdb.set_trace()
            urls = valid_a_href(a_elements, url)
            not_exist = self.url_dedup.insert_not_exist(urls)
            #self.db_helper.insert_mining_task(task, urls)
            self.db_helper.insert_mining_task(task, not_exist)
        #print url, body[:100]

    def process_error(self, failure, task):
        url = task.get('url')
        print failure.getErrorMessage()
        self.logger.error("download error, url:%s, msg:%s" %
                (url, failure.getTraceback()))

    def process_task(self, task):
        #url = url.encode('utf-8')
        url = task.get('url').encode('utf-8')
        requestProcess = (self.process_request, (task,), {})
        bodyProcess = (self.process_body, (task,), {})
        errorProcess = (self.process_error, (task,), {})

        #print "process_task:", url
        if self.use_pool:
            self.pool.download_and_process(url, bodyProcess)
        else:
            self.reactor.download_and_process(url, None, requestProcess, bodyProcess, errorProcess, redirect=True)

    def start(self, performance=False):
        if not performance:
            first_task = self.db_helper.init_mining_job(self.init_url, continue_run=False)
            self.process_task(first_task)
            while True:
                try:
                    tasks = self.db_helper.get_mining_task(self.task_number)
                    #print 'mining task', tasks
                    for task in tasks:
                        self.process_task(task)
                    time.sleep(self.get_delay)
                except KeyboardInterrupt:
                    sys.exit(0)
                except Exception as e:
                    print e

        else:
            total = 0
            print datetime.now()
            while True:
                tasks = list(self.db_helper.task_co.find().sort('_id', 1).skip(total).limit(self.task_number))
                total += self.task_number
                if total > 150000:
                    print datetime.now()
                    break
                #print 'mining task', tasks
                for task in tasks:
                    try:
                        task['depth'] = 10
                        self.process_task(task)
                    except KeyboardInterrupt:
                        sys.exit(0)
                    except Exception as e:
                        print e, args
                if not self.use_pool:
                    time.sleep(self.get_delay)

def main():
    #signal.signal(signal.SIGINT, lambda : sys.exit(0))
    #signal.signal(signal.SIGTERM, lambda : sys.exit(0))

    logging.config.fileConfig("../conf/log_mining_crawler.conf")
    reactor = HttpReactor()
    threadpool = HttpThreadpool(30, 200)
    config = ConfigParser.ConfigParser()
    config.read('../conf/mining_crawler.conf')
    init_url = [
            'http://news.qq.com/',
            'http://news.163.com/',
            'http://news.sina.com.cn/',
            'http://news.ifeng.com/',
            'http://news.sohu.com/',
            'http://www.xinhuanet.com/',
            ]
    init_url = ['http://news.qq.com/']
    for url in init_url:
        miner_server = MinerServer(reactor, threadpool, [url], config, True)
        t = threading.Thread(target=miner_server.start, args=(True,))
        t.setDaemon(True)
        t.start()

    url = 'http://sports.163.com/'
    #first_task = miner_server.db_helper.init_mining_job(url)
    #miner_server.process_task(first_task)

    reactor.run()

if __name__ == '__main__':
    main()
