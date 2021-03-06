#!/usr/bin/env python
# coding=gbk
from pprint import pformat

from http_reactor import HttpReactor
from queue_service import BlockingQueueService
from seed_scheduler import SeedScheduler, SeedHandler
from item import SeedTask
from hub_extractor import HubExtractor
from article_server import ArticleServer

import logging, logging.config, signal, sys, threading, Queue, ConfigParser
from urllib import urlencode

class HubServer(object):

    def __init__(self, reactor, queue_service, conf):
        self.logger = logging.getLogger("")
        self.reactor = reactor
        self.queue_service = queue_service
        self.hub_extractor = HubExtractor(conf)

        # FIXME delete article server
        self.article_server = ArticleServer(reactor, conf)

    def process_request(self, response, url):
        return
        self.logger.info("http response, url:%s, code:%s, phrase:%s, headers:%s" %
                (url, response.code, response.phrase,
                pformat(list(response.headers.getAllRawHeaders()))))

    def process_body(self, body, url):
        self.logger.info("page body, url:%s, body:%s" %
                (url, body[:100]))
        not_exist = self.hub_extractor.extract(body, url)
        if not not_exist:
            print not_exist
        for url in not_exist:
            self.article_server.process_task(url)
        #print body[:100]

    def process_error(self, failure, url):
        print failure.getErrorMessage()
        self.logger.error("download error, url:%s, msg:%s" %
                (url, failure.getErrorMessage()))

    def process_task(self, url):
        url = url.encode('utf-8')
        requestProcess = (self.process_request, (url,), {})
        bodyProcess = (self.process_body, (url,), {})
        errorProcess = (self.process_error, (url,), {})

        #print "process_task:", url
        self.reactor.download_and_process(url, None, requestProcess, bodyProcess, errorProcess, redirect=True)

    def start(self):
        while True:
            try:
                task, msg = self.queue_service.get(10)
                if task:
                    url = task.url
                    self.process_task(url)
                else:
                    print 'queue empty'
            except KeyboardInterrupt:
                sys.exit(0)
            except Exception as e:
                print e
                pass

def main():
    #signal.signal(signal.SIGINT, lambda : sys.exit(0))
    #signal.signal(signal.SIGTERM, lambda : sys.exit(0))

    logging.config.fileConfig("../conf/seed_log.conf")
    conf = dict(address="localhost", port=10010, db_name="news_crawler")
    queue_service = BlockingQueueService(100)
    handler = SeedHandler(queue_service)
    scheduler = SeedScheduler('background', handler, conf)
    scheduler.start()

    reactor = HttpReactor()
    config = ConfigParser.ConfigParser()
    config.read('../conf/url_dedup.conf')
    hubserver = HubServer(reactor, queue_service, config)
    t = threading.Thread(target=hubserver.start)
    t.daemon = True
    t.start()

    url = "http://roll.news.sina.com.cn/s/channel.php?ch=01#col=89&spec=&type=&ch=01&k=&offset_page=0&offset_num=0&num=60&asc=&page=1"
    url = "http://www.163.com/"
    for _ in xrange(2):
        queue_service.put(SeedTask(url), 1)
    hubserver.process_task(url)
    reactor.run()

if __name__ == '__main__':
     main()
