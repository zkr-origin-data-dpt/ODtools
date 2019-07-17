import random
import sys
import time
import traceback
from ODtools.fdfs_client.client import Fdfs_client
from ODtools.singleton_tools import Singleton


class FastDfsClient(metaclass=Singleton):
    """
    fastdfs 工具类
    """
    def __init__(self, tracker_host: str = None, tracker_port: int = None):
        """
        :param tracker_host: tracker地址
        :param tracker_port: tracker端口
        """
        self.trackers = {
            'host_tuple': (tracker_host, ),
            'port': tracker_port,
            'timeout': 15,
            'name': 'Tracker Pool'
        }
        self.client = Fdfs_client(trackers=self.trackers)

    def save_file(self, filepath: str):
        """
        保存文件
        :param filepath: 本地文件路径
        :return:
        """
        trash = 3
        while trash != 0:
            try:
                result = self.client.upload_by_filename(filepath)
                if result['Status'] == 'Upload successed.':
                    return self.check_exception(result)
                else:
                    raise ConnectionError
            except BaseException as e:
                time.sleep(random.random())
                print('fasdfs exception: line:%s :%s' % (sys.exc_info()[2].tb_lineno, traceback.format_exc()))
                trash -= 1
                self.client = Fdfs_client(trackers=self.trackers)

        if trash == 0:
            # print('fuck fasdfs down')
            # raise TIOError
            # return ''
            raise Exception("fuck fastdfs down")

    def save_buffer(self, file_buffer: bytes, file_ext_name: str):
        """
        保存文件流
        :param file_buffer: bytes流
        :param file_ext_name: 文件格式
        :return:
        """
        trash = 3
        while trash != 0:
            try:
                result = self.client.upload_by_buffer(filebuffer=file_buffer, file_ext_name=file_ext_name)
                if result['Status'] == 'Upload successed.':
                    return self.check_exception(result)
                else:
                    continue
            except BaseException as e:
                time.sleep(random.random())
                print('fasdfs exception: line:%s :%s' % (sys.exc_info()[2].tb_lineno, traceback.format_exc()))
                trash -= 1
                self.client = Fdfs_client(trackers=self.trackers)

        if trash == 0:
            # print('fuck fasdfs down')
            # raise TIOError
            # return ''
            raise Exception("fuck fastdfs down")

    def delete_file(self, dfs_name: str):
        """
        删除文件
        :param dfs_name: 上传的文件路径
        :return:
        """
        trash = 3
        while trash != 0:
            try:
                result = self.client.delete_file(dfs_name)
                return result
            except BaseException as e:
                time.sleep(random.random())
                if "No such file or directory" in str(e):
                    return e
                print('fasdfs exception: line:%s :%s' % (sys.exc_info()[2].tb_lineno, e))
                trash -= 1
                self.client = Fdfs_client(trackers=self.trackers)

        if trash == 0:
            # print('fuck fasdfs down')
            # raise TIOError
            # return ''
            raise Exception("fuck fastdfs down")

    def check_exception(self, result: dict):
        """
        检查是否上传成功
        :param result:
        :return:
        """
        dfs_name = result['Remote file_id'].decode()
        if 'group' in dfs_name:
            return dfs_name
        else:
            raise Exception("file upload fail")


if __name__ == '__main__':
    pass
