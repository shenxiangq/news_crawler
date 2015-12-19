#!/usr/bin/env python
# coding=gbk

import logging, logging.config, signal, sys, threading, Queue, ConfigParser

from http_reactor import HttpReactor
from article_extractor import ArticleExtractor

class ArticleServer(object):

    def __init__(self, reactor, conf):
        self.reactor = reactor
        self.article_extractor = ArticleExtractor(conf)
        self.logger = logging.getLogger("")

    def process_request(self, response, url):
        pass

    def process_body(self, body, url):
        self.article_extractor.extract(body, url)
        print body[:100]

    def process_error(self, failure, url):
        print failure.getErrorMessage()
        self.logger.error("download error, url:%s, msg:%s" %
                (url, failure.getErrorMessage()))

    def process_task(self, url):
        url = url.encode('utf-8')
        requestProcess = (self.process_request, (url,), {})
        bodyProcess = (self.process_body, (url,), {})
        errorProcess = (self.process_error, (url,), {})

        print "article_task:", url
        self.reactor.download_and_process(url, None, requestProcess, bodyProcess, errorProcess, redirect=True)

