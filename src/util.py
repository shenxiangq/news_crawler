# coding=utf-8

import logging
from tld import get_tld
import chardet, sys
import functools

def init_log(filename):
    logging.basicConfig(filename)

def valid_a_href(a_elements, main_url=None):
    hrefs = [a.get('href') for a in a_elements]
    hrefs = [link for link in hrefs if link and link.startswith('http://')]
    if main_url:
        main_tld = get_tld(main_url, fail_silently=True)
        hrefs = [link for link in hrefs if get_tld(link, fail_silently=True) == main_tld]

    return hrefs

def to_unicode(bytes_str):
    '''
    >>> print to_unicode('中文编码')
    中文编码
    '''
    error_rate = 8.5
    gbk = bytes_str.decode('gbk', 'replace')
    utf = bytes_str.decode('utf8', 'replace')
    gbk_count = gbk.count(u'\ufffd')
    utf_count = utf.count(u'\ufffd')
    if gbk_count * error_rate <= utf_count:
        return gbk.replace(u'\ufffd', '')
    else:
        return utf.replace(u'\ufffd', '')


def retry(max_tries=3, logger=None):
    def deco_retry(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            remain = max_tries
            while remain > 0:
                try:
                    return func(*args, **kw)
                except KeyboardInterrupt:
                    sys.exit(1)
                except Exception as e:
                    print e
                remain -= 1
            return func(*args, **kw)
        return wrapper
    return deco_retry

