from pprint import pformat

from twisted.internet import reactor
from twisted.internet.task import react
from twisted.web.client import Agent, readBody, RedirectAgent
from twisted.web.http_headers import Headers


def cbRequest(response, url=None):
    print url
    print 'Response version:', response.version
    print 'Response code:', response.code
    print 'Response phrase:', response.phrase
    print 'Response headers:'
    print pformat(list(response.headers.getAllRawHeaders()))
    d = readBody(response)
    d.addCallback(cbBody)
    return d

def cbBody(body):
    print 'Response body:',
    print len(body), body[:100]

def main(url="http://roll.news.sina.com.cn/s/channel.php?ch=01"):
    agent = RedirectAgent(Agent(reactor))
    d = agent.request(
        'GET', url,
        Headers({'User-Agent': ['Twisted Web Client Example']}),
        None)
    d.addCallback(cbRequest, url)
    d = agent.request(
        'GET', url,
        Headers({'User-Agent': ['Twisted Web Client Example']}),
        None)
    d.addCallback(cbRequest)

main()
reactor.run()
