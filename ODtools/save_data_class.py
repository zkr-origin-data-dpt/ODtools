# -*- coding: utf-8 -*-
# @Author   : FJ
import json
import time
import hashlib
import pymysql


class SaveOriginalData(object):
    def __init__(self, username: str, password: str, ip: str, host: int=3306, db: str="fangjie"):
        '''
        :param username: mysql username
        :param password: mysql password
        :param ip: mysql ip
        :param host: mysql host
        '''
        self.data_source = {
                "weibo_info": "original_data_weibo_info",
                "weibo_user": "original_data_weibo_user",
                "news_info": "original_data_news_info",
                "wechat_info": "original_data_wechat_info",
                "wechat_user": "original_data_wechat_user",
            }
        self.username = username
        self.password = password
        self.ip = ip
        self.host = host
        self.db = db
        for i in range(3):
            try:
                self.db = pymysql.connect(host=self.ip, port=self.host, user=self.username, password=self.password, db=self.db)
                self.cursor = self.db.cursor()
                print("Instantiation SQLClient success")
                break
            except:
                continue
        else:
            raise

    def get_rowkey(self, data):
        return data["rowkey"]

    def get_m_images(self, data):
        return data.get("m_images", "")

    def get_m_videos(self, data):
        if data.get("m_videos", ''):
            return data.get("m_videos", "")
        else:
            return data.get("m_video_url", "")

    def save(self, original_data, data_type):
        sql = "insert into {} (id,url,images,videos,data,data_update_time) values(%s,%s,%s,%s,%s,%s)".format(self.data_source[data_type])
        data_update_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        try:
            # 传来的是json
            if type(original_data) == str:
                print(1)
                data = json.loads(original_data)
                url = self.get_rowkey(data)
                id = hashlib.md5(url.encode()).hexdigest()
                self.cursor.execute(sql, (id, url, self.get_m_images(data), self.get_m_videos(data), json.dumps(data), data_update_time))
            # 传来的是字典
            elif type(original_data) == dict:
                print(2)
                data = original_data
                url = self.get_rowkey(data)
                id = hashlib.md5(url.encode()).hexdigest()
                self.cursor.execute(sql, (id, url, self.get_m_images(data), self.get_m_videos(data), json.dumps(data), data_update_time))
            # 传来的是列表或集合或元组
            else:
                print(3)
                for one_data in original_data:
                    if type(one_data) == str:
                        data = json.loads(one_data)
                        url = self.get_rowkey(data)
                        id = hashlib.md5(url.encode()).hexdigest()
                        self.cursor.execute(sql, (id, url, self.get_m_images(data), self.get_m_videos(data), json.dumps(data), data_update_time))
                    elif type(one_data) == dict:
                        data = one_data
                        url = self.get_rowkey(data)
                        id = hashlib.md5(url.encode()).hexdigest()
                        self.cursor.execute(sql, (id, url, self.get_m_images(data), self.get_m_videos(data), json.dumps(data), data_update_time))
            self.db.commit()
            print("原始数据入库成功")
            return True
        except Exception as e:
            self.db.rollback()
            print("存储出错, 错误原因:{}".format(e))

    def __del__(self):
        self.cursor.close()
        self.db.close()
        print("实例化释放")

if __name__ == '__main__':
    from ODtools import HBaseClient
    # 测试
    hb = HBaseClient("192.168.129.231", 9090)
    table_name = "NEWS_INFO_TABLE"
    result = hb.scan_result(table_name)
    sql_clict = SaveOriginalData("zkr_cj", "zkrcj123", "192.168.129.224", 3306, db="fangjie")
    num = 1
    for i in result:
        if i.get("m_project_name", "") == "yqsj15":
        # if i.get("m_project_name", "") == "cbfx62":
            print(i)
            print(num)
            sql_clict.save(i, "news_info")
            num += 1
            if num >= 100:
                break