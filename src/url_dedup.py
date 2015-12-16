#!/usr/bin/env python
# coding=gbk

from pybloom import BloomFilter
from datetime import datetime
import ConfigParser, logging, logging.config, hashlib
from bson.binary import Binary

import pymongo

class URLDedup(object):

    def __init__(self, conf):
        self.load_conf(conf)

    def load_conf(self, conf):
        self.use_bloom = conf.getboolean('bloom', 'use_bloom')
        bloom_capacity = conf.getint('bloom', 'capacity')
        bloom_error_rate = conf.getfloat('bloom', 'error_rate')
        host = conf.get('db', 'host')
        database = conf.get('db', 'database')
        port = conf.getint('db', 'port')
        collection = conf.get('db', 'collection')
        self.collection = pymongo.MongoClient(host, port)[database][collection]
        if self.use_bloom:
            self.bloom_filter = BloomFilter(bloom_capacity, bloom_error_rate)

    def hash_url(self, url):
        return hashlib.md5(url).digest()

    def insert_db(self, url, hash_url):
        '''
        return True, url exists.
        '''
        obj = dict(url=url, md5=Binary(hash_url))
        rs = self.collection.insert_one(obj)
        if rs is not None:
            return True
        else:
            return False

    def query_db(self, url, hash_url):
        rs = self.collection.find_one({'md5': Binary(hash_url)})
        return rs

    def add(self, url):
        '''
        return True, url exists.
        '''
        hash_url = self.hash_url(url)
        if self.use_bloom:
            self.bloom_filter.add(hash_url)
        self.insert_db(url, hash_url)

    def check(self, url):
        hash_url = self.hash_url(url)
        if self.use_bloom:
            if hash_url not in self.bloom_filter:
                return False
        return self.query_db(url, hash_url) is not None


def main():
    conf = ConfigParser.ConfigParser()
    conf.read('../conf/url_dedup.conf')
    url_dedup = URLDedup(conf)
    n = 10**5
    import time
    prefix="www.xxx.com"
    start = time.time()
    for i in xrange(n):
        url_dedup.add(prefix+str(i))

    end = time.time()
    print 'total time:%.3f' % (end - start)
    print '%.3f / second' % (n/(end-start))

    start = time.time()
    for i in xrange(n):
        url_dedup.check(prefix+str(i))

    end = time.time()
    print 'total time:%.3f' % (end - start)
    print '%.3f / second' % (n/(end-start))

if __name__ == '__main__':
    main()

