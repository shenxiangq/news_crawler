#!/usr/bin/env python
# coding=gbk

import logging, logging.config, signal, sys, time, threading, Queue, ConfigParser, urllib2
from pprint import pformat
from datetime import datetime
from lxml.html import HTMLParser, document_fromstring
import lxml.html

from db_helper import DBHelper
from util import valid_a_elements, get_html_path

class SeedMiner(object):

    def __init__(self, conf):
        self.db_helper = DBHelper(conf)
        self.parser = HTMLParser(encoding='utf-8', remove_comments=True, remove_blank_text=True)

    def read_html(self, object_id):
        try:
            object_id = 'index.html'
            fin = open('../data/mining_page/'+object_id)
            body = fin.read()
            body = body.decode('gbk').encode('utf-8')
            tree = document_fromstring(body, parser=self.parser)
            return tree
        except IOError:
            return None

    def maybe_hub(self, url, tree):
        a_elements = valid_a_elements(tree.xpath('//a'), url)
        visited_a = set()
        all_a = set(a_elements)
        a_elements = [a for a in a_elements if a.text and len(a.text.strip()) >= 10]
        block = []
        max_div = 2
        max_depth = 8
        import pdb;pdb.set_trace()
        for start_a in a_elements:
            if start_a in visited_a:
                continue
            path = '/a'
            iter_node = start_a
            div_count = 0
            loop_flag = True
            for _ in xrange(max_depth):
                if not loop_flag:
                    break
                if div_count > max_div or iter_node.tag == 'body':
                    break
                iter_node = iter_node.getparent()
                if iter_node.tag in ('div', 'ul') and len(iter_node.getchildren()) > 1:
                    div_count += 1
                    sibling = iter_node.xpath('.'+path)
                    if len(sibling) >= 3 and \
                        all([x in all_a for x in sibling]):
                        block.append((iter_node, path))
                        [visited_a.add(x) for x in sibling]
                        loop_flag = False

                path = '/' + iter_node.tag + path

        paths = []
        for node, path in block:
            paths.append(get_html_path(node) + path)
        print len(block)
        import pdb;pdb.set_trace()
        for node, path in block:
            ss = lxml.html.tostring(node)

        return block


    def test(self):
        for obj in self.db_helper.get_some_mining_task():
            url = obj.get('url')
            _id = str(obj.get('_id'))
            tree = self.read_html(_id)
            tree.make_links_absolute(url)
            if tree is not None:
                self.maybe_hub(url, tree)

def main():
    conf = ConfigParser.ConfigParser()
    conf.read('../conf/mining_crawler.conf')
    miner = SeedMiner(conf)
    miner.test()

if __name__ == '__main__':
    main()
