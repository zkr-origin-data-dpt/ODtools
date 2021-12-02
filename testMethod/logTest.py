#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@File  : logTest.py
@Author: longfei 
@Date  : 2021/6/29 2:54 下午
@Desc  : 
'''
import sys

import requests

from ODtools.monitor_tools_new import Source, Project, Mrequest
from ODtools.redis_tools import RedisClient

sys.path.append("../")

# a = base_logger(log_name="integertor--{}".format("1"),
#                     file_path="./", mode="DEBUG",
#                     cmd_output=True)
# for i in range(100):
#     a.debug("test.1")
#     time.sleep(2)
url = "http://python123.io"
proxy = {
    "http": "http://42.6.114.99:5417"
}
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36 Edg/95.0.1020.44"
}
res = requests.head(url, headers=headers)
kv = {'key1': 'value1'}
# res = requests.request('POST', 'http://python123.io/ws', json=kv)
# res = requests.request('HEAD', 'http://python123.io/ws')
# res = requests.request('GET', 'http://python123.io/ws')
print(res.headers, res.raw)
rd = RedisClient(host="192.168.129.196", port=6379, cluster=False).redis

mrequest_class = Mrequest(rd, statistic=True)
spider_record_dict = {
    'project_name': Project.OTHER,
    'source_name': Source.OTHER,
    'component_name': "{}_{}_app".format("test", "requests")
}
print(spider_record_dict)
response = mrequest_class.mrequest(url, "get", spider_record_dict, headers=headers, proxies=proxy, timeout=40)
print(response.headers,response.status_code)
