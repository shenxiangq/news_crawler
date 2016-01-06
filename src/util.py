# coding=utf-8

import logging, re, copy
import chardet, sys
import functools
from tld import get_tld

def init_log(filename):
    logging.basicConfig(filename)

def valid_a_href(a_elements, main_url=None):
    hrefs = [a.get('href') for a in a_elements]
    hrefs = [link for link in hrefs if link and link.startswith('http://')]
    if main_url:
        main_tld = get_tld(main_url, fail_silently=True)
        hrefs = [link for link in hrefs if get_tld(link, fail_silently=True) == main_tld]

    return hrefs

def valid_a_elements(a_elements, main_url=None):
    al = [a for a in a_elements if a.get('href') and a.get('href').startswith('http://')]
    if main_url:
        main_tld = get_tld(main_url, fail_silently=True)
        al = [a for a in al if get_tld(a.get('href'), fail_silently=True) == main_tld]
    return al

def get_html_path(node):
    path_node = []
    iter_node = node
    while iter_node is not None and iter_node.tag != 'body':
        path_node.append(iter_node.tag)
        iter_node = iter_node.getparent()
    path_node.append('html')
    path_node.append('')
    path_node.reverse()
    return '/'.join(path_node)

NONE_STYLE_RX = re.compile(r'display\s*:\s*none')
def remove_display_none(tree):
    to_remove = []
    for e in tree.iter():
        style = e.get('style')
        if style and NONE_STYLE_RX.search(style):
            e.drop_tree()
    return tree

def p_raw_content(raw_p):
    p = copy.deepcopy(raw_p)
    for e in p.iter():
        if e.tag == 'a':
            e.drop_tag()
    return p.text_content() or ''

def contains_tag(tag, tag_list):
    for e in tag.iter():
        if e in tag_list:
            return True
    return False

CHINESE_CODE_RX = re.compile(ur'[\u4e00-\u9fa5]+')
def extract_chinese_code(unicode_str):
    if isinstance(unicode_str, unicode):
        return u''.join(CHINESE_CODE_RX.findall(unicode_str))
    else:
        raise ValueError('must be unicode string')

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

