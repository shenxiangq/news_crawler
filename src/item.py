
class SeedTask(object):
    def __init__(self, url):
        self.url = url

class MinerTask(object):
    def __init__(self, url, time, job_id):
        self.url = url
        self.time = time
        self.job_id = job_id
