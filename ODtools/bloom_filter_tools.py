from hashlib import md5


class SimpleHash(object):
    def __init__(self, cap, seed):
        self.cap = cap
        self.seed = seed

    def hash(self, value):
        ret = 0
        for i in range(len(value)):
            ret += self.seed * ret + ord(value[i])
        return (self.cap - 1) & ret


class BloomFilter(object):
    def __init__(self, redis_client, block_num: int = 1, key: str = 'bloomfilter'):
        """
        bloom filter
        :param redis_client: redis client
        :param block_num: block num
        :param key: redis key
        """
        self.bit_size = 1 << 31  # Redis的String类型最大容量为512M，现使用256M
        self.seeds = [5, 7, 11, 13, 31]
        # self.seeds = [5, 7, 11, 13, 31, 37, 61]
        self.server = redis_client
        # self.server = redis.StrictRedis(host=host, port=port, decode_responses=True)
        self.key = key
        self.blockNum = block_num
        self.hash_func = []
        for seed in self.seeds:
            self.hash_func.append(SimpleHash(self.bit_size, seed))

    def is_contains(self, str_input: str):
        """
        bloom filter contains string or not
        :param str_input:
        :return:
        """
        if not str_input:
            return False
        m5 = md5()
        m5.update(str_input.encode())
        str_input = m5.hexdigest()
        ret = True
        name = self.key + str(int(str_input[0:2], 16) % self.blockNum)
        for f in self.hash_func:
            loc = f.hash(str_input)
            ret = ret & self.server.getbit(name, loc)
        return ret

    def insert(self, str_input: str):
        """
        insert string to bloom filter
        :param str_input:
        :return:
        """
        m5 = md5()
        m5.update(str_input.encode())
        str_input = m5.hexdigest()
        name = self.key + str(int(str_input[0:2], 16) % self.blockNum)
        for f in self.hash_func:
            loc = f.hash(str_input)
            self.server.setbit(name, loc, 1)


if __name__ == '__main__':
    pass
