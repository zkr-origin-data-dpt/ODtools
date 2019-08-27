import time
import random
from ODtools.fdfs_client.client import Fdfs_client
from ODtools.singleton_tools import Singleton


class FastDfsClient(metaclass=Singleton):
    """
    fastdfs tools client
    """
    def __init__(self, tracker_host: str = None, tracker_port: int = None):
        """
        :param tracker_host: tracker host
        :param tracker_port: tracker port
        """
        self.trackers = {
            'host_tuple': (tracker_host, ),
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
                return result[0]
            except Exception as e:
                if "No such file or directory" in str(e):
                    return str(e)
                if i != trash - 1:
                    time.sleep(random.random())
                    self.client = Fdfs_client(trackers=self.trackers)
                    continue
                else:
                    raise e

    @staticmethod
    def check_exception(result: dict) -> str:
        """
        check exception
        :param result:
        :return:
        """
        dfs_name = result['Remote file_id'].decode()
        if 'group' in dfs_name:

            from ODtools import RedisClient
            from datetime import datetime
            r_pipe = RedisClient(host='127.0.0.1', port=6379, cluster=False).pipe
            dfs_size = result['Uploaded size']
            if dfs_size.endswith('B'):
                dfs_size = dfs_size[:-1]
            elif dfs_size.endswith('KB'):
                dfs_size = dfs_size[:-2] * 1024
            elif dfs_size.endswith('MB'):
                dfs_size = dfs_size[:-2] * 1024 * 1024
            elif dfs_size.endswith('GB'):
                dfs_size = dfs_size[:-2] * 1024 * 1024 * 1024
            else:
                dfs_size = 0
            dfs_suffix = result['Remote file_id'].decode().split('.')[-1]
            key = 'fdfs_status_' + format(datetime.now(), "%Y-%m-%d")
            r_pipe.hincrby(key, dfs_suffix, dfs_size)
            r_pipe.expire(key, 24 * 3600 * 3)
            r_pipe.execute()

            return dfs_name
        else:
            raise Exception("Upload fail.")


if __name__ == '__main__':
    pass
