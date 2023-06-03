import redis

class RedisQ:
    def __init__(self, host, port, q_name):
        '''Simple Redis reliable queue client implementation'''
        self.redis_conn = redis.Redis(host=host, port=port)
        self.main_q = q_name
        self.processing_q = q_name + ":processing"

    def main_q_size():
        '''Return length of main queue'''
        return self.redis_conn.llen(self.main_q)

    def processing_q_size():
        '''Return length of processing queue'''
        return self.redis_conn.llen(self.processing_q)

    def get(block=True):
        '''Pop item from tail of main queue and push it to head of processing queue'''
        if block:
            item = self.redis_conn.blmove(self.main_q, self.processing, src="right", dest="left")
        else:
            item = self.redis_conn.lmove(self.main_q, self.processing, src="right", dest="left")
        return item

    def complete(item):
        '''Remove item from tail of processing queue'''
        self.redis_conn.lrem(self.processing_q, -1, item)
