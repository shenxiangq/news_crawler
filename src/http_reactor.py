#!/usr/bin/env python
# coding=gbk

from twisted.internet import reactor, task
from twisted.web.client import (Agent, readBody, RedirectAgent, BrowserLikeRedirectAgent,
                                HTTPConnectionPool, )
from twisted.web.http_headers import Headers
from twisted.python import log

class HttpReactor(object):

    def __init__(self, reactor_type='default'):
        pool = HTTPConnectionPool(reactor)
        self.agent = Agent(reactor, pool)
        self.redirect_agent = RedirectAgent(Agent(reactor))
        #self.redirect_agent = BrowserLikeRedirectAgent(Agent(reactor))

    def download_and_process(self, url, headers, requestProcess, bodyProcess, errProcess, method='GET',
            redirect=False, retry=3, delay=10):
        if headers is None:
            headers = Headers({'User-Agent': ['Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.93 Safari/537.36']})
        reactor.callFromThread(self._download, url, headers, requestProcess, bodyProcess, errProcess, method, redirect, retry, delay)


    def _download(self, url, headers, requestProcess, bodyProcess, errProcess, method='GET',
            redirect=False, retry=2, delay=10):
        if redirect:
            agent = self.redirect_agent
        else:
            agent = self.agent
        func = lambda :agent.request(method, url, headers).addCallback(
                self.handler_request, requestProcess, bodyProcess)
        retry_call = RetryCall(retry, delay, func)
        self.retry_process(retry_call, errProcess, func)

    def handler_request(self, response, requestProcess, bodyProcess):
        request_func, request_args, request_kw = requestProcess
        body_func, body_args, body_kw = bodyProcess
        rs = request_func(response, *request_args, **request_kw)
        d = readBody(response)
        d.addCallback(body_func, *body_args, **body_kw)
        return d

    def retry_process(self, retry_call, errProcess, func, *args, **kw):
        d = func(*args, **kw)
        d.addErrback(self.retry_err_callback, retry_call, errProcess)

    def retry_err_callback(self, failure, retry_call, errProcess):
        err_func, err_args, err_kw = errProcess
        err_func(failure, *err_args, **err_kw)
        try:
            log.msg('error retry %d remaining' % retry_call.remaining)
            d = retry_call.next()
        except ValueError:
            log.msg('max retry')
        else:
            d.addErrback(self.retry_err_callback, retry_call, errProcess)

    def run(self):
        reactor.run()


class RetryCall(object):
    def __init__(self, retry, delay, func, *args, **kw):
        self.remaining = retry
        self.delay = delay
        self.func = func
        self.args = args
        self.kw = kw

    def next(self):
        if self.remaining > 0:
            self.remaining -= 1
            d = task.deferLater(reactor, self.delay, self.func, *self.args, **self.kw)
            return d
        else:
            raise ValueError("end of iter.")


def main():
    reac = HttpReactor()
    def empty_func(obj):
        pass
    def error_func(body):
        print str(body)[:100]
        print "fail once"
        raise Exception("must fail")
    error_process = (error_func, [], {})
    empty_process = (empty_func, [], {})
    reac.download_and_process("http://www.baidu.com", None, empty_process, error_process, empty_process)
    reac.run()

if __name__ == '__main__':
    main()
