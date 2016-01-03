from http_reactor import HttpReactor
from util import to_unicode
import lxml.html

def process_request(response, url):
    print response.code
    import pdb;pdb.set_trace()
    return

def process_body(body, url):
    import pdb;pdb.set_trace()
    body = to_unicode(body)
    doc = lxml.html.fromstring(body)


def process_error(failure, url):
    print failure.getErrorMessage()

def main():
    reactor = HttpReactor()

    url = 'http://3g.163.com/news/16/0101/00/BC70TOEK00014AED.html'
    requestProcess = (process_request, (url,), {})
    bodyProcess = (process_body, (url,), {})
    errorProcess = (process_error, (url,), {})

    reactor.download_and_process(url, None, requestProcess, bodyProcess, errorProcess, redirect=True)
    reactor.run()


if __name__ == '__main__':
     main()
