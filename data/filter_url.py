#!/bin/awk -f
import re, sys

rx=re.compile("(v.qq.com/cover/./.+.html)|v.ifeng.com/(fashion|news|ent|mil|vblog)/.+/[0-9]+/.{20,}.shtml|(.{2}.house.163.com/.{2}/.{4}.html)|tv.sohu.com/\d{8}/n\d+.shtml|my.tv.sohu.com/us/\d+/\d+.shtml|v.163.com/paike/V.+/V.+.html")

filename = sys.argv[1]
with open(filename) as fin:
    for line in fin:
        if not rx.search(line):
            print line,

