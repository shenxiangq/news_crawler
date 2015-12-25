#!/usr/bin/env python
# coding=gbk

from pybloom import BloomFilter
from datetime import datetime
import ConfigParser, logging, logging.config, hashlib
from bson.binary import Binary

import pymongo

class URLDedup(object):

    def __init__(self, conf, collection=None):
        self.collection = collection
        self.load_conf(conf)

    def load_conf(self, conf):
        '''
        self.use_bloom = conf.getboolean('bloom', 'use_bloom')
        bloom_capacity = conf.getint('bloom', 'capacity')
        bloom_error_rate = conf.getfloat('bloom', 'error_rate')
        '''
        host = conf.get('db', 'host')
        database = conf.get('db', 'database')
        port = conf.getint('db', 'port')
        if self.collection is None:
            self.collection = conf.get('db', 'dedup_collection')
        self.collection = pymongo.MongoClient(host, port)[database][self.collection]

    def hash_url(self, url):
        return Binary(hashlib.md5(url).digest())

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

    def insert_db_many(self, url_list, hash_list):
        binary_list = [Binary(x) for x in hash_list]
        objs = [dict(md5=Binary(x)) for x in hash_list]
        self.collection.insert_many(objs)

    def query_db_many(self, hash_list):
        rs = self.collection.find({'md5': {'$in': hash_list}})
        md5_list = [item.get('md5') for item in list(rs)]
        return md5_list

    def add_many(self, url_list):
        hash_list = [self.hash_url(url) for url in url_list ]
        if hash_list:
            self.insert_db_many(url_list, hash_list)

    def check_many(self, url_list):
        hash_to_url = {self.hash_url(url): url for url in url_list}
        md5_list = self.query_db_many(hash_to_url.keys())
        md5_set = set(md5_list)
        not_exist = [hash_to_url.get(md5) for md5 in hash_to_url if md5 not in md5_set]
        return not_exist

    def insert_not_exist(self, url_list):
        not_exist = self.check_many(url_list)
        self.add_many(not_exist)
        return not_exist

def main():
    conf = ConfigParser.ConfigParser()
    conf.read('../conf/url_dedup.conf')
    url_dedup = URLDedup(conf)
    n = 10**5
    import time
    prefix="www.xxx.com"
    urls = []
    for i in xrange(n):
        urls.append(prefix+str(i))
    start = time.time()
    url_dedup.add_many(urls)
    end = time.time()
    print 'total time:%.3f' % (end - start)
    print '%.3f / second' % (n/(end-start))

    for i in xrange(n):
        urls.append(prefix+str(i-100))

    start = time.time()
    not_exist = url_dedup.insert_not_exist(urls)
    end = time.time()
    print 'total time:%.3f' % (end - start)
    print '%.3f / second' % (n/(end-start))
    print 'not exist:', len(not_exist)

if __name__ == '__main__':
    main()

