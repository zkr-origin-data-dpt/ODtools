import random

from ODtools.monitor_tools_new import Mrequest
from ODtools.monitor_tools_new import Source, Project
from ODtools.redis_tools import RedisClient
from loguru import logger

redis = RedisClient(host=random.choice(["192.168.129.195"]), port=random.choice([6379]),
                    cluster=False, password=None).redis
mrequest_class = Mrequest(redis, statistic=True)

spider_record_dict = {
    'project_name': Project.OTHER,
    'source_name': Source.WEIBO,
    'component_name': "{}_weibo_pc".format("62"),
    'project_name_num': "62"
}


def getHeaders():
    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
        "Connection": "close",
    }
    return headers


def getProxy(rd, logger, redisKey="BJsjzzH:Proxy:weibo"):
    """获取代理的方法
    """
    proxy = rd.lrange("ProxyL", 0, -1)
    if proxy:
        proxy = random.choice(list(proxy))
        proxy = {"http": proxy, "https": proxy}
    else:
        proxys = [
            "http://106.15.251.14:10621",
            "http://106.15.251.14:10623",
            "http://106.15.251.14:10625",
        ]
        proxy = random.choice(proxys)
        proxy = {}

    # proxy = "http://123.163.116.147:20090"
    logger.info('INFO!proxy_mv 拿到的代理是--{}'.format(proxy))
    return proxy


def reqeusts_method():
    url = "https://wwww.baidu.com"
    res = mrequest_class.mrequest(url, "get", spider_record_dict,
                                  proxies={"http": "http://127.0.0.1:8080", "https": "http://127.0.0.1:8080"})
    print(res.status_code)


def ansync_method():
    proxy = getProxy(redis, logger).get("http")

    response_list = mrequest_class.aio_request(["https://www.baidu.com", "https://www.qq.com"],
                                               spider_record_dict,
                                               response_model='html',
                                               headers=getHeaders(),
                                               proxy=proxy,
                                               timeout=20,
                                               )
    print(response_list)

if __name__ == '__main__':
    ansync_method()
