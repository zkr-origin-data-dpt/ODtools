import random

import redis
import rediscluster
from ODtools.singleton_tools import Singleton


class RedisClient(metaclass=Singleton):
    """
    redis tools client
    """

    def __init__(self, host, port, cluster: bool = False, password=None, **kwargs):
        """
        :param host: redis host
        :param port: redis port
        :param cluster: is redis cluster or not
        """
        startup_nodes = []
        self.host = random.choice(host) if type(host) == list else host
        self.port = random.choice(port) if type(port) == list else int(port)
        if type(self.port) == str:
            self.port = int(self.port)
        if cluster:
            if type(host) == list and type(port) == list:
                for one_host in host:
                    for one_port in port:
                        startup_nodes.append(
                            {"host": one_host, "port": int(one_port) if type(one_port) == str else one_port})
            if startup_nodes:
                self.redis = rediscluster.RedisCluster(
                    startup_nodes=startup_nodes,
                    password=password,
                    decode_responses=True,
                    socket_connect_timeout=5, **kwargs
                )
            else:
                self.redis = rediscluster.RedisCluster(
                    host=self.host,
                    port=self.port,
                    password=password,
                    decode_responses=True,
                    socket_connect_timeout=5, **kwargs
                )
        else:
            self.redis = redis.StrictRedis(host=self.host,
                                           port=self.port,
                                           password=password,
                                           decode_responses=True,
                                           socket_connect_timeout=5, **kwargs)


if __name__ == '__main__':
    redis_host = ['172.16.67.29', '172.16.67.30', '172.16.67.31']

    redis_port = ['6379', '6380']
    redis_cluster = True
    password = None
    # redis_host = ['172.16.67.32',]
    # redis_port = ['6379']
    # redis_cluster = False
    # password = "1qaz!QAZ2wsx"
    a = RedisClient(redis_host, redis_port,redis_cluster,max_connections=10,password=password).redis
    # a = RedisClient("192.168.129.213",6400,False).redis
    print(a.info())
    if redis_cluster:
        for k,_info in a.info().items():
            print(_info.get("connected_clients"))
    else:
        print(a.info().get("connected_clients"))
    print(a.ping())
    print(a.pipeline())
    print(a.scard("SdL:L:Hot_links"))
