from pymongo import MongoClient

def get_db():
    port = 10010
    name = "news_crawler"
    return MongoClient("localhost", port)[name]

def insert():
    rs = []
    db = get_db()
    co = db['seed']
    with open("seed.txt") as fin:
        line = fin.readline().strip()
        URL, INTER, JOBID = line.split("\t")
        co.remove({})
        co.create_index(JOBID)
        for line in fin:
            line = line.strip()
            url, interval, jobid = line.split("\t")
            interval = int(interval)
            jobid = int(jobid)
            rs.append({URL: url, INTER: interval, JOBID: jobid})
        print rs
        obj_ids = co.insert(rs)
        print obj_ids

if __name__ == '__main__':
    insert()
