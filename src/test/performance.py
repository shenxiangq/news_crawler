import time, chardet, functools, re, hashlib, random
from chardet.universaldetector import UniversalDetector

def timetest(repeat=1):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            start = time.time()
            for _ in xrange(repeat):
                result = func(*args, **kw)
            end = time.time()
            print 'total time:%.3f' % (end - start)
            print '%.3f / second' % (repeat/(end-start))
            return result

        return wrapper
    return decorator

@timetest(10)
def to_unicode(bytes_str):
    error_rate = 8.5
    gbk = bytes_str.decode('gbk', 'replace')
    utf = bytes_str.decode('utf8', 'replace')
    gbk_count = gbk.count(u'\ufffd')
    utf_count = utf.count(u'\ufffd')
    if gbk_count * error_rate <= utf_count:
        return gbk.replace(u'\ufffd', '')
    else:
        return utf.replace(u'\ufffd', '')

def error_dist(strs):
    uu = strs.decode('gbk')
    gbk_count = uu.encode('gbk').decode('utf8','replace').count(u'\ufffd')
    utf_count = uu.encode('utf8').decode('gbk','replace').count(u'\ufffd')
    print gbk_count, utf_count

@timetest(10*6)
def md5_hash(url):
    url += str(random.random())
    s = hashlib.md5(url)
    return s.digest()


def main():
    #body = ''.join(open('index.html', 'rb').readlines())
    #print to_unicode(body)[:1000]
    #print error_dist(body)
    md5_hash('http://ahttps://docs.python.org/3/library/functools.html')

if __name__ == '__main__':
    main()
