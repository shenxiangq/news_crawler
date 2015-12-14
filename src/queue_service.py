import Queue

class BlockingQueueService(object):
    def __init__(self, maxsize=1000):
        self.q = Queue.Queue(maxsize=maxsize)

    def try_put(task):
        try:
            self.q.put(task, False)
            return True, 'success'
        except Queue.Full:
            return False, 'queue full'


    def put(timeout):
        try:
            self.q.put(task, True, timeout)
            return True, 'success'
        except Queue.Full:
            return False, 'queue full'

    def try_get():
        try:
            task = self.q.get(False)
            return task, 'success'
        except queue.empty:
            return none, 'queue empty'

    def get(timeout):
        try:
            task = self.q.get(True, timeout)
            return True, 'success'
        except Queue.Full:
            return False, 'queue full'



