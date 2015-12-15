#!/usr/bin/env python
# coding=gbk

from twisted.internet import reactor
from twisted.web.client import Agent, readBody, RedirectAgent, BrowserLikeRedirectAgent
from twisted.web.http_headers import Headers

class HttpReactor(object):

    def __init__(self, reactor_type='default'):
        self.agent = Agent(reactor)
        #self.redirect_agent = RedirectAgent(Agent(reactor))
        self.redirect_agent = BrowserLikeRedirectAgent(Agent(reactor))

    def download_and_process(self, url, headers, requestProcess, bodyProcess, errProcess, method='GET', redirect=False):
        if redirect:
            agent = self.redirect_agent
        else:
            agent = self.agent
        err_func, err_args, err_kw = errProcess
        d = agent.request(method, url, headers)
        d.addCallback(self.handler_request, requestProcess, bodyProcess).addErrback(err_func, *err_args, **err_kw)

    def handler_request(self, response, requestProcess, bodyProcess):
        request_func, request_args, request_kw = requestProcess
        body_func, body_args, body_kw = bodyProcess
        request_func(response, *request_args, **request_kw)
        d = readBody(response)
        d.addCallback(body_func, *body_args, **body_kw)
        return d

    def run(self):
        reactor.run()



