import random

import redis
import rediscluster
from ODtools.singleton_tools import Singleton


class RedisClient(metaclass=Singleton):
    """
    redis tools client
    """

    def __init__(self, host: str, port: int, cluster: bool = False, password: str = False):
        """
        :param host: redis host
        :param port: redis port
        :param cluster: is redis cluster or not
        """
        self.host = random.choice(host) if type(host) == list else host
        self.port = random.choice(port) if type(port) == list else port
        redis_type = rediscluster.StrictRedisCluster if cluster else redis.StrictRedis
        if password:
            self.redis = redis_type(host=self.host,
                                    port=self.port,
                                    password=password,
                                    decode_responses=True,
                                    socket_connect_timeout=5)
        else:
            self.redis = redis_type(host=self.host,
                                port=self.port,
                                decode_responses=True,
                                socket_connect_timeout=5)

        self.pipe = self.redis.pipeline()


if __name__ == '__main__':
    a = RedisClient("192.168.129.212",6390,False,"xjzfw2020").redis
    # a = RedisClient("192.168.129.213",6400,False).redis
    print(a.info())
