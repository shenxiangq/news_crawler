from pymongo import MongoClient
import datetime
from hashlib import md5
from bson.binary import Binary
import time

def get_db():
    port = 10010
    name = "news_crawler"
    return MongoClient("localhost", port)[name]

def insert():
    rs = []
    db = get_db()
    co = db['seed']
    with open("seed.txt") as fin:
        line = fin.readline().strip()
        URL, INTER, JOBID = line.split("\t")
        co.remove({})
        co.create_index(JOBID)
        for line in fin:
            line = line.strip()
            url, interval, jobid = line.split("\t")
            interval = int(interval)
            jobid = int(jobid)
            rs.append({URL: url, INTER: interval, JOBID: jobid})
        print rs
        obj_ids = co.insert(rs)
        print obj_ids

def url_dedup():
    db = get_db()
    co = db['url_dup']
    url = 'http://www.xxx.com'
    co.remove({})
    co.insert({'url': url, 'md5':Binary(md5(url).digest()), 'last_access': datetime.datetime.now()})
    co.create_index([('md5','hashed')])

def test_dedup():
    # insert test
    n = 10**5
    prefix = 'http://www.xxx.com/'
    db = get_db()
    co = db['url_dup']
    co.remove({})
    now = datetime.datetime.now()
    co.create_index([('md5','hashed')])
    objs = []
    for i in xrange(n):
        url = prefix + str(i)
        b = Binary(md5(url).digest())
        objs.append({'md5': b})
        #co.insert_one({'url': url, 'md5': b, 'last_access': now })
    start = time.time()
    co.insert_many(objs)
    end = time.time()
    print 'total time:%.3f' % (end - start)
    print '%.3f / second' % (n/(end-start))

    query = []
    now = datetime.datetime.now()
    for i in xrange(n):
        url = prefix + str(i)
        b = Binary(md5(url).digest())
        query.append(b)
    start = time.time()
    rs = co.find({'md5': {'$in': query}})
    list(rs)
    end = time.time()
    print 'total time:%.3f' % (end - start)
    print '%.3f / second' % (n/(end-start))


if __name__ == '__main__':
    #insert()
    #url_dedup()
    test_dedup()
