import redis
import rediscluster
from tools.singleton_tools import Singleton


class RedisClient(metaclass=Singleton):
    """
    redis 工具类
    """
    def __init__(self, host: str, port: int, cluster: bool = False):
        """
        :param host:
        :param port:
        :param cluster: 是否为集群版redis
        """
        self.host = host
        self.port = port
        redis_type = rediscluster.StrictRedisCluster if cluster else redis.StrictRedis

        self.redis = redis_type(host=self.host, port=self.port, decode_responses=True,
                                socket_connect_timeout=5)
        self.pipe = self.redis.pipeline()
