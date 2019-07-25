import redis
import rediscluster
from ODtools.singleton_tools import Singleton


class RedisClient(metaclass=Singleton):
    """
    redis tools client
    """
    def __init__(self, host: str, port: int, cluster: bool = False):
        """
        :param host: redis host
        :param port: redis port
        :param cluster: is redis cluster or not
        """
        self.host = host
        self.port = port
        redis_type = rediscluster.StrictRedisCluster if cluster else redis.StrictRedis

        self.redis = redis_type(host=self.host,
                                port=self.port,
                                decode_responses=True,
                                socket_connect_timeout=5)

        self.pipe = self.redis.pipeline()


if __name__ == '__main__':
    pass
