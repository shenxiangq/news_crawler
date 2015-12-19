#!/usr/bin/env python
# coding=gbk

import lxml.html
import logging, logging.config
from readability.readability import Document

from mongo_connection import get_conn

class ArticleExtractor(object):

    def __init__(self, conf):
        self.logger = logging.getLogger("")
        self.db = get_conn('localhost', 10010, 'news_crawler')


    def extract(self, body, url):
        doc = Document(body)
        title = doc.short_title()
        content = doc.summary()
        self.save_article(url, title, content)
        self.logger.info('Article extract, title=%s, content=%s' % (title, content[:100]))

    def save_article(self, url, title, content):
        collection = self.db['article']
        collection.insert(dict(url=url, title=title, content=content))

