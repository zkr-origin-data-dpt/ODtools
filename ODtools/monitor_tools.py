# _ * _coding: utf - 8_ * _
# @Author: LHK
# @date: 2021/6/2 16:20
# @File : test_monitor_class.py


import asyncio
import time
from datetime import datetime
from enum import Enum

import requests
from aiohttp import ClientSession
from aiosocksy.connector import ProxyClientRequest, ProxyConnector
from apscheduler.schedulers.blocking import BlockingScheduler
from redis import Redis
from rediscluster import RedisCluster


# 把超时时间的设置
def spider_record(client, record_dict: dict = dict, step: int = 1, del_record: bool = False):
    """0
    增加计数器到数据库中,并根据需求判断是否将计数器设置销毁时间为第二天零点
    :param client: 数据库连接对象
    :param record_dict: 形式为{'general_info': xxx, 'record_info': xxx},字典的key必须是general_info和record_info
    :param step: 计数器一次性增加的数量
    :param del_record: 是否需要设置计数器第二天零点销毁
    :return:
    """
    project_name = record_dict['project_name']
    source_name = record_dict['source_name']
    record_info = record_dict['record_info']
    if not isinstance(project_name, Project): raise TypeError('error data type project_name')
    if not isinstance(source_name, Source): raise TypeError('error data type source_name')
    general_info = '{}:{}'.format(project_name.value, source_name.value)
    today_date = get_today_str()
    # 为计数器的key添加当前日期前缀
    if general_info and today_date not in general_info:
        general_info = today_date + '_' + general_info
    if record_info and today_date not in record_info:
        record_info = today_date + '_' + record_info
    # 判断redis大key当前的存活时间,将其销毁时间置为明天0点
    # 频繁操作redis对redis的key做判断
    if general_info:
        client.hincrby(general_info, record_info, step)
        if del_record:
            # 判断计数器是否设置了销毁时间
            if not client.ttl(general_info): client.expireat(general_info, get_tomorrow_timestamp())
    else:
        client.incrby(record_info, step)
        if del_record:
            # 判断计数器是否设置了销毁时间
            if not client.ttl(record_info): client.expireat(record_info, get_tomorrow_timestamp())


def get_tomorrow_timestamp() -> int:
    """
    获取第二天零点的时间戳
    :return: 明天00:00:00的时间戳
    """
    return int(time.mktime(time.strptime(time.strftime('%Y-%m-%d', time.localtime(time.time() + 86400)), '%Y-%m-%d')))


def get_today_str() -> str:
    """
    获取当前日期的字符串,格式为2021-01-01
    :return:
    """
    return datetime.today().strftime('%Y-%m-%d')


class Project(Enum):
    """项目名称"""
    CBFX = 'Cbfx'
    TSGZ = 'Tsgz'
    YBYQ = 'Ybyq'
    NMYQ = 'Nmyq'
    GDSY = 'Gdsy'
    OTHER = 'Other'


class Source(Enum):
    """项目数据源"""
    WEIBO = 'Weibo'
    WECHAT = 'Wechat'
    DOUYIN = 'Douyin'
    NEWS = 'News'
    TIEBA = 'Tieba'
    APP = 'App'
    HOTLIST = 'HotList'
    TOUTIAO = 'Toutiao'
    OTHER = 'OtherSource'


class Response(Enum):
    """错误的数据链接请求"""
    status_code = 500


class Task(object):
    """
    任务调度分发类,主要分为定时分发任务、循环分发任务、单次分发任务
    """

    def __init__(self, db_client=None, *, db_host: str = None, db_port: int = 6379, db_cluster: bool = False,
                 db_password: str = None):
        """
        传入数据库连接配置,创建数据库连接对象
        :param db_client: 数据库连接客户端,如果传入这个参数那么后面的参数无需传入
        :param db_host: 数据库连接地址
        :param db_port: 数据库连接端口
        :param db_cluster: 数据库是否是集群
        :param db_password: 数据库密码
        """
        if db_client:
            self.db_client = db_client
        else:
            redis_type = RedisCluster if db_cluster else Redis
            self.db_client = redis_type(host=db_host, port=db_port, password=db_password)

    def cron_job(self, que_name: str, task_data: list, *, hour: str = None, minute: str = None, second: str = None):
        """
        定时分发任务,任务队列类型必须是列表类型
        :param que_name: 任务的队列名称
        :param task_data: 待分发的任务列表
        :param hour: 任务定时分发的小时数
        :param minute: 任务定时分发的分钟数
        :param second: 任务定时分发的秒数
        :return:
        """
        scheduler = BlockingScheduler(timezone="Asia/Shanghai")
        scheduler.add_job(
            lambda que_name, task_data: self.db_client.rpush(que_name, *task_data), 'cron',
            hour=hour,
            minute=minute,
            second=second,
            args=[que_name, task_data]
        )
        scheduler.start()

    def cyclic_job(self, que_name: str, task_data: list, second: int):
        """
        循环分发任务,任务队列类型必须是列表类型
        :param que_name: 分发任务队列的名称
        :param task_data: 待分发任务的任务列表
        :param second: 任务分发的时间间隔,单位(秒)
        :return:
        """
        while True:
            self.db_client.rpush(que_name, *task_data)
            print('task distribution complete...')
            time.sleep(second)

    def single_job(self, que_name: str, task_data: list, que_type: str = 'list', sort: str = 'r'):
        """
        单次任务分发,队列可以是列表或集合
        :return:
        """
        if que_type == 'set':
            self.db_client.sadd(que_name, *task_data)
        else:
            if sort == 'r':
                self.db_client.rpush(que_name, *task_data)
            else:
                self.db_client.lpush(que_name, *task_data)


class Mrequest(object):
    """
    发送请求的类,分为同步和异步
    """

    def __init__(self, db_client=None, statistic=False, *, db_host: str = None, db_port: int = 6379,
                 db_cluster: bool = False,
                 db_password: str = None):
        """
        传入数据库连接配置,创建数据库连接对象
        :param db_client: 数据库连接客户端,如果传入这个参数那么后面的参数无需传入
        :param statistic: 是否需要开启计数功能
        :param db_host: 数据库连接地址
        :param db_port: 数据库连接端口
        :param db_cluster: 数据库是否是集群
        :param db_password: 数据库密码
        """
        self.statistic = statistic
        if db_client:
            self.db_client = db_client
        else:
            redis_type = RedisCluster if db_cluster else Redis
            self.db_client = redis_type(host=db_host, port=db_port, password=db_password)

    def mrequest(self, urls: str, method: str = 'get', record_dict: dict = dict, step: int = 1, **kwargs):
        """
        使用requests同步发送请求,参数分别为目标任务url,请求方法,计数器字典,计数步长
        """
        if self.statistic:
            component_name = record_dict['component_name']
        else:
            component_name = None
        try:
            request_method = getattr(requests, method.lower())
        except AttributeError:
            raise AttributeError('no method named {}'.format(method))
        try:
            response = request_method(urls, **kwargs)
            if response.status_code == 200:
                if self.statistic: record_dict['record_info'] = '{}_request_success'.format(component_name)
            else:
                if self.statistic: record_dict['record_info'] = '{}_request_fail'.format(component_name)
            if self.statistic: spider_record(self.db_client, record_dict, step, False)
            return response
        except Exception as e:
            print(e)
            if self.statistic:
                record_dict['record_info'] = '{}_request_fail'.format(component_name)
                spider_record(self.db_client, record_dict, step, False)
            print('失败计数加一')
            return Response

    def aio_request(self, urls: list, record_dict: dict = dict, response_model: str = 'html', step: int = 1,
                    cookies=None, **kwargs):
        """
        异步发送请求,使用aiohttp.请求参数分别是: 目标任务url列表,计数器字典,请求响应格式,计数器步长
        :return:
        """
        if self.statistic:
            component_name = record_dict['component_name']

        async def async_request(url, cookies=None, **kwargs):
            if cookies:
                clientSessionObj = ClientSession(connector=ProxyConnector(), request_class=ProxyClientRequest,
                                                 cookies=cookies)
            else:
                clientSessionObj = ClientSession(connector=ProxyConnector(), request_class=ProxyClientRequest)
            async with clientSessionObj as session:
                try:
                    async with session.get(url, **kwargs) as response:
                        if response.status == 200:
                            if self.statistic: record_dict['record_info'] = '{}_request_success'.format(component_name)
                        else:
                            if self.statistic: record_dict['record_info'] = '{}_request_fail'.format(component_name)
                        if self.statistic: spider_record(self.db_client, record_dict, step, False)
                        if response_model == 'json':
                            return await response.json(), str(response.url), response
                        elif response_model == 'bytes':
                            return await response.read(), str(response.url), response
                        elif response_model == 'html':
                            return await response.text(), str(response.url), response
                        else:
                            return response, str(response.url), response
                except Exception as e:
                    if self.statistic:
                        record_dict['record_info'] = '{}_request_fail'.format(component_name)
                        spider_record(self.db_client, record_dict, step, False)
                    import traceback
                    print(traceback.format_exc(), url)
                    return '', url, Response

        tasks = [asyncio.ensure_future(async_request(i, cookies=cookies, **kwargs)) for i in urls]
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait(tasks))
        del loop
        return [i.result() for i in tasks]


class Parse(object):
    """
    解析源数据类,使用思路为继承该类并重写analysis_data方法
    """

    def __init__(self, db_client=None, statistic=False, *, db_host: str = None, db_port: int = 6379,
                 db_cluster: bool = False,
                 db_password: str = None):
        self.statistic = statistic
        if db_client:
            self.db_client = db_client
        else:
            redis_type = RedisCluster if db_cluster else Redis
            self.db_client = redis_type(host=db_host, port=db_port, password=db_password)

    # @time_counter
    def analysis_data(self, source_code, record_dict: dict = dict, step: int = 1, *args, **kwargs):
        """
        在新的类中调用analysis_data方法时需要调用super().analysis_data
        :param source_code:
        :param record_dict:
        :return:
        """
        if self.statistic:
            component_name = record_dict['component_name']
            record_dict['record_info'] = '{}_analysis_count'.format(component_name)
            spider_record(self.db_client, record_dict, step)


class Monitor(object):

    def __init__(self, rds=None, statistic=False, *, redis_host: str = None, redis_port: int = 6379,
                 redis_cluster: bool = False,
                 redis_password: str = None):
        self.statistic = statistic
        if rds:
            self.rds = rds
        else:
            redis_type = RedisCluster if redis_cluster else Redis
            self.rds = redis_type(host=redis_host, port=redis_port, password=redis_password)

    def log(self):
        """日志统计"""
        ...

    # 在收集的大key前面添加日期，之后删除被收集的大key
    def statistics_count(self, summary_keys: dict, *, hour: str = None, minute: str = None, second: str = None):
        """
        收集指定的大key计数到统计的key中
        :param summary_keys: 形式是{big_key1: [key1, key2, key3]}
        :param hour:
        :param minute:
        :param second:
        :return:
        """

        def summary_count(summary_keys: dict):
            today_date = get_today_str()
            for summary_key, big_keys in summary_keys.items():
                for big_key in big_keys:
                    count_data = self.rds.hgetall(big_key)
                    for k, v in count_data.items():
                        if today_date not in summary_key:
                            summary_key = today_date + '_' + summary_key
                        self.rds.hset(summary_key, k, v)

        scheduler = BlockingScheduler(timezone="Asia/Shanghai")
        scheduler.add_job(
            summary_count, 'cron',
            hour=hour,
            minute=minute,
            second=second,
            args=[summary_keys]
        )
        scheduler.start()
