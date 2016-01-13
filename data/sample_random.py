import sys, random, commands

infile = sys.argv[1]
outfile = sys.argv[2]
number = int(sys.argv[3])

count = int(commands.getoutput('cat '+infile+'|wc -l'))
sample = set(random.sample(xrange(count), number))

with open(infile) as fin, open(outfile, 'w') as fout:
    for i, line in enumerate(fin):
        if i in sample:
            fout.write(line)
