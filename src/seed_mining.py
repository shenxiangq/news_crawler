#!/usr/bin/env python
# coding=gbk

import logging, logging.config, signal, sys, time, threading, Queue, ConfigParser, urllib2, re, traceback
from pprint import pformat
from datetime import datetime
from lxml.html import HTMLParser, document_fromstring, fromstring
import lxml.html
from readability.readability import Document

from db_helper import DBHelper
from util import (valid_a_elements, get_html_path, remove_display_none,
            p_raw_content, contains_tag, extract_chinese_code)

BLOCK_TAG = ('div', 'ul')

class SeedMiner(object):

    def __init__(self, conf):
        self.db_helper = DBHelper(conf)
        self.parser = HTMLParser(encoding='utf-8', remove_comments=True, remove_blank_text=True)
        self.cleaner = lxml.html.clean.Cleaner(style=True, scripts=True,
            page_structure=False, safe_attrs_only=False, comments=True, javascript=True)

    def read_html(self, object_id, url):
        # object_id = 'index.html'
        try:
            fin = open('../data/mining_page/'+object_id)
        except IOError:
            return None
        body = fin.read()
        fin.close()
        #body = body.decode('gbk').encode('utf-8')
        tree = document_fromstring(body, parser=self.parser)
        tree = remove_display_none(tree)
        tree = self.cleaner.clean_html(tree)
        tree.make_links_absolute(url)
        return tree

    def maybe_hub(self, url, tree):
        block, matched_a = self.get_hub_block(url, tree)
        tree = self.remove_p_aside_a(block, tree)
        content_tree = self.get_readability_content(url, tree)
        content = unicode(content_tree.text_content().strip())
        content = re.sub(ur'\s', u'', content)
        chinese_content = extract_chinese_code(content)
        a_content = sum([len(a.text.strip()) for a in matched_a])
        ratio = len(chinese_content)*1.0/(a_content or 0.001)
        print 'url:%s match content/link:%f' % (url, ratio)
        print len(content), content.encode('utf-8')

        #import pdb;pdb.set_trace()
        if len(matched_a) > 20 and len(chinese_content) < 150 and ratio < 0.2:
            return True
        else:
            return False

    def get_readability_content(self, url, tree):
        body = lxml.html.tostring(tree)
        doc = Document(body)
        content = doc.summary(True)
        content_tree = fromstring(content, parser=self.parser)
        return content_tree

    def get_hub_block(self, url, tree):
        a_elements = valid_a_elements(tree.xpath('//a'), url)
        visited_a = set()
        all_a = set(a_elements)
        long_a = set([a for a in a_elements if a.text and len(a.text.strip()) >= 10])
        block = []
        max_div = 2
        max_depth = 8
        min_link_number = 3
        for start_a in long_a:
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
                if iter_node is None:
                    break
                if iter_node.tag in BLOCK_TAG and len(iter_node.getchildren()) > 1:
                    div_count += 1
                    sibling = iter_node.xpath('.'+path)
                    if len(sibling) >= min_link_number and \
                        all([x in all_a for x in sibling]):
                        long_a_sibling = [x for x in sibling if x in long_a]
                        block.append((iter_node, path, long_a_sibling))
                        [visited_a.add(x) for x in sibling]
                        loop_flag = False

                path = '/' + iter_node.tag + path

        matched_a = [a for a in long_a if a in visited_a]

        paths = []
        for node, path, long_a in block:
            paths.append(get_html_path(node) + path)
        print len(block)
        #import pdb;pdb.set_trace()
        for node, path, long_a in block:
            ss = lxml.html.tostring(node)

        return block, matched_a

    def remove_p_aside_a(self, block, tree):
        for node, path, long_a in block:
            for e in node.iter():
                text = e.text or ''
                text = text.strip()
                if len(text) < 100 and e not in long_a:
                    e.text = ''

        return tree

    def test(self):
        fout =  open('../data/mining_result.txt', 'w')
        for obj in self.db_helper.get_some_mining_task(0, 10000):
            url = obj.get('url')
            _id = str(obj.get('_id'))
            tree = self.read_html(_id, url)
            if tree is not None:
                try:
                    rs = self.maybe_hub(url, tree)
                    fout.write(str(rs) + ' ' + url + '\n')
                except:
                    print "ERROR!"
                    traceback.print_exc()
            else:
                fout.write('\n')
        fout.close()

def main():
    conf = ConfigParser.ConfigParser()
    conf.read('../conf/mining_crawler.conf')
    miner = SeedMiner(conf)
    miner.test()

if __name__ == '__main__':
    main()
