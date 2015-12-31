
from concurrent.futures import ThreadPoolExecutor
import urllib2
from util import retry, to_unicode

class HttpThreadpool(object):

    def __init__(self, max_workers=10, queue_size=200):
        self.executor = ThreadPoolExecutor(max_workers, queue_size)

    @retry(max_tries=3)
    def _download(self, url):
        req = urllib2.Request(url)
        req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.93 Safari/537.36')
        rsp = urllib2.urlopen(req, timeout=5)
        return rsp.read()

    def download_and_process(self, url, body_process):
        return self.executor.submit(self._download_and_process, url, body_process)

    def _download_and_process(self, url, body_process):
        body_func, body_args, body_kw = body_process
        body = self._download(url)
        body_func(body, *body_args, **body_kw)

    def shutdown(self):
        self.executor.shutdown()

def main():
    def print_body(body):
        body = to_unicode(body)
        print body[:500]
    bodyProcess = (print_body, [], {})
    pool = HttpThreadpool(1, None)
    #pool._download('http://www.baidu.com')
    url_list = [
            'http://news.qq.com/',
            'http://baike.baidu.com/view/13897.htm',
            #'http://www.jb51.net/article/46641.htm',
            #'http://www.cnblogs.com/yuxc/archive/2011/08/01/2123995.html',
            #'http://my.oschina.net/leejun2005/blog/179265',
            ]
    for url in url_list:
        print url
        f = pool.download_and_process(url, bodyProcess)
        print f.result()
    print 'done'
    pool.shutdown()


if __name__ == '__main__':
    main()
