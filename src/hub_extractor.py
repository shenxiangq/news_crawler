#!/usr/bin/env python
# coding=gbk

import lxml.html
import logging, logging.config

from url_dedup import URLDedup
from mongo_connection import get_conn
from util import valid_a_href

class HubExtractor(object):

    def __init__(self, conf):
        self.url_dedup = URLDedup(conf)
        self.logger = logging.getLogger("")
        self.db = get_conn('localhost', 10010, 'news_crawler')

    def extract(self, body, url):
        tree = lxml.html.document_fromstring(body)
        a_elements = tree.xpath('//a')
        urls = valid_a_href(a_elements)
        self.save_result(url, urls)
        not_exist = self.url_dedup.insert_not_exist(urls)
        self.logger.info('not exist urls. urls=%s' % str(not_exist))
        return not_exist

    def save_result(self, url, extract_urls):
        collection = self.db['seed_task']
        collection.insert({'url': url, 'extract_urls': extract_urls})


