from http_reactor import HttpReactor
from util import to_unicode

def process_request(response, url):
    print response.code
    import pdb;pdb.set_trace()
    return

def process_body(body, url):
    import pdb;pdb.set_trace()
    body = to_unicode(body)


def process_error(failure, url):
    print failure.getErrorMessage()

def main():
    reactor = HttpReactor()

    url = 'http://news.sohu.com/'
    requestProcess = (process_request, (url,), {})
    bodyProcess = (process_body, (url,), {})
    errorProcess = (process_error, (url,), {})

    reactor.download_and_process(url, None, requestProcess, bodyProcess, errorProcess, redirect=True)
    reactor.run()


if __name__ == '__main__':
     main()
