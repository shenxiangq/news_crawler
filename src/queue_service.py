import Queue

class BlockingQueueService(object):
    def __init__(self, maxsize=1000):
        self.q = Queue.Queue(maxsize=maxsize)

    def try_put(self, task):
        try:
            self.q.put(task, False)
            return True, 'success'
        except Queue.Full:
            return False, 'queue full'


    def put(self, task, timeout):
        try:
            self.q.put(task, True, timeout)
            return True, 'success'
        except Queue.Full:
            return False, 'queue full'

    def try_get(self):
        try:
            task = self.q.get(False)
            return task, 'success'
        except queue.Empty:
            return None, 'queue empty'

    def get(self, timeout):
        try:
            task = self.q.get(True, timeout)
            return task, 'success'
        except Queue.Empty:
            return None, 'queue full'



