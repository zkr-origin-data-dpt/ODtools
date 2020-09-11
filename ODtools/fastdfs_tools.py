import json
import time
import random

import requests

from ODtools.fdfs_client.client import Fdfs_client
from ODtools.singleton_tools import Singleton


class FastDfsClient(metaclass=Singleton):
    """
    fastdfs tools client
    """

    def __init__(self, tracker_host: str = None, tracker_port: int = None, tracker_read_port: int = 8083):
        """
        :param tracker_host: tracker host fastdfs ip
        :param tracker_port: tracker port fastdfs save data port
        :param tracker_read_port: tracker_read_port port fastdfs read port
        """
        self.tracker_read_api = "http://{}:{}".format(tracker_host, tracker_read_port)
        self.trackers = {
            'host_tuple': (tracker_host,),
            'port': tracker_port,
            'timeout': 15,
            'name': 'Tracker Pool'
        }
        self.client = Fdfs_client(trackers=self.trackers)

    def save_file(self, filepath: str) -> str:
        """
        save file
        :param filepath: local file path
        :return: fastdfs path
        """
        trash = 3
        for i in range(trash):
            try:
                result = self.client.upload_by_filename(filepath)
                if result['Status'] == 'Upload successed.':
                    return self.check_exception(result)
                else:
                    raise ConnectionError
            except Exception as e:
                if i != trash - 1:
                    self.client = Fdfs_client(trackers=self.trackers)
                    continue
                else:
                    raise e

    def save_buffer(self, file_buffer: bytes, file_ext_name: str) -> str:
        """
        save file bytes buffer
        :param file_buffer: bytes buffer
        :param file_ext_name: file suffix
        :return: fastdfs path
        """
        trash = 3
        for i in range(trash):
            try:
                result = self.client.upload_by_buffer(filebuffer=file_buffer, file_ext_name=file_ext_name)
                print(result)
                if result['Status'] == 'Upload successed.':
                    return self.check_exception(result)
                else:
                    continue
            except Exception as e:
                if i != trash - 1:
                    self.client = Fdfs_client(trackers=self.trackers)
                    continue
                else:
                    raise e

    def delete_file(self, dfs_name: str) -> str:
        """
        delete fastdfs file
        :param dfs_name: fastdfs file path
        :return:
        """
        trash = 3
        for i in range(trash):
            try:
                result = self.client.delete_file(dfs_name)
                return result[0] + " " + dfs_name
            except Exception as e:
                if "No such file or directory" in str(e):
                    return str(e) + "  " + dfs_name
                if i != trash - 1:
                    time.sleep(random.random())
                    self.client = Fdfs_client(trackers=self.trackers)
                    continue
                else:
                    raise e

    def saveInfo(self, fileInfo, hbase, hbaseTable: str, hbaseItem: dict, file_ext_name=None):
        """
        :param existsInfo:  已存在的数据
        :param relatedRowkey:  关联信息rowkey
        :param sourceType:  数据来源
        :return:
        """
        # 判断是否已经存储过
        rowkey = hbaseItem.get("rowkey")
        relatedRowkey = hbaseItem.get("relatedRowkey")
        relatedTable = hbaseItem.get("relatedTable")
        sourceType = hbaseItem.get("sourceType")
        dataType = hbaseItem.get("dataType", "info")
        existsInfo = None
        try:
            existsInfo = hbase.get_result(rowkey, hbaseTable)
        except BaseException as e:
            return {"Status": -1, "address": "", "msg": "get hbase info error hbaseTale is ok"}
        if existsInfo:
            relatedInfo = existsInfo.get(sourceType, "{}")
            try:
                relatedInfo = json.loads(relatedInfo)
            except BaseException as e:
                relatedInfo = eval(relatedInfo)
            for k, v in relatedInfo.items():
                if k == relatedRowkey:  # 如果已有数据
                    address = v.get("address", "")
                    if address.startswith("group"):
                        blockAddress = "{}/{}".format(self.tracker_read_api, address)  # 测试能否访问，如果不可以放弃，重新存储
                        try:
                            res = requests.get(blockAddress)
                            if res.status_code == 200 and res.headers.get("Content-Length"):
                                return {"Status": 1, "address": address, "msg": "已有数据"}
                            else:
                                self.delete_file(address)
                        except BaseException as e:
                            pass
        # 开始进行数据保证并更新或创建hbase 表信息
        try:
            address = self.save_file(fileInfo) if not file_ext_name else self.save_buffer(fileInfo, file_ext_name)
        except BaseException as e:
            import traceback
            return {"Status": -1, "address": "", "msg": traceback.format_exc()}

        if existsInfo:  # 数据更新
            relatedInfo[relatedRowkey] = {"address": address, "table": relatedTable}
            existsInfo[sourceType] = json.dumps(relatedInfo)
            hbase.put_result(rowkey, existsInfo, hbaseTable, "wa")

        else:
            newsInfo = {"rowkey": rowkey, "type": dataType}
            newsInfo[sourceType] = json.dumps({relatedRowkey: {"address": address, "table": relatedTable}})
            hbase.put_result(rowkey, newsInfo, hbaseTable, "wa")
        return {"Status": 1, "address": address, "msg": "新增数据"}

    def save_file_hbase(self, filePath: str, hbase, hbaseTable: str, hbaseItem: dict) -> dict:
        """
        save file
        :param filepath: local file path
        :return: fastdfs path
        """
        baseDictKey = {"rowkey", "dataTrype", "relatedRowkey", "relatedTable", "sourceType"}  # 需要满足的字段值
        Subtraction = set(baseDictKey) - set(hbaseItem)
        # 判断数据完整度
        if Subtraction:
            return {"Status": -1, "address": "", "msg": "bhaseItem lack keys - {}".format(Subtraction)}
        # 判断用户类型
        sourceType = hbaseItem.get("sourceType")
        if sourceType not in ["caiji", "suanfa"]:
            return {"Status": -1, "address": "", "msg": "error sourceType value should be caiji or suanfa"}

        return self.saveInfo(filePath, hbase, hbaseTable, hbaseItem)

    def save_buffer_hbase(self, fileBuffer: bytes, fileExtName: str, hbase, hbaseTable: str,
                          hbaseItem: dict) -> dict:
        """
        save file
        :param filepath: local file path
        :return: fastdfs path
        """
        baseDictKey = {"rowkey", "dataTrype", "relatedRowkey", "relatedTable", "sourceType"}  # 需要满足的字段值
        Subtraction = set(baseDictKey) - set(hbaseItem)
        # 判断数据完整度
        if Subtraction:
            return {"Status": -1, "address": "", "msg": "bhaseItem lack keys - {}".format(Subtraction)}
        # 判断用户类型
        sourceType = hbaseItem.get("sourceType")
        if sourceType not in ["caiji", "suanfa"]:
            return {"Status": -1, "address": "", "msg": "error sourceType value should be caiji or suanfa"}
        return self.saveInfo(fileBuffer, hbase, hbaseTable, hbaseItem, fileExtName)

    @staticmethod
    def check_exception(result: dict) -> str:
        """
        check exception
        :param result:
        :return:
        """
        dfs_name = result['Remote file_id'].decode()
        if 'group' in dfs_name:
            return dfs_name
        else:
            raise Exception("Upload fail.")



if __name__ == '__main__':
    pass