import random
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport
from ODtools.hbase_client import *
from ODtools.hbase_client.ttypes import *
from ODtools.singleton_tools import Singleton


class HBaseClient(metaclass=Singleton):
    """
    HBase 工具类
    """
    def __init__(self, hbase_address: str, hbase_port: int, hbase_servers: list = None):
        """
        :param hbase_address:
        :param hbase_port:
        :param hbase_servers:
        """
        self.address = hbase_address
        self.port = hbase_port
        self.servers = hbase_servers
        self.client = self.init_client(self.address, self.port)

    def reconnect(self):
        """
        重新连接hbase
        :return:
        """
        if self.servers:
            h_a, h_p = random.choice(self.servers)
            self.client = self.init_client(h_a, h_p)
        else:
            self.client = self.init_client(self.address, self.port)

    def init_client(self, address, port):
        """
        实例化hbase客户端
        :param address:
        :param port:
        :return:
        """
        transport = TSocket.TSocket(address, port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        transport.open()
        client = THBaseService.Client(protocol)
        return client

    def get_result(self, hbase_row: str, hbase_table: str) -> dict:
        """
        查询
        :param hbase_row: rowkey
        :param hbase_table: 表名
        :return:
        """
        trash = 5
        while trash != 0:
            try:
                values = {}
                get = TGet()
                get.row = hbase_row.encode()
                result = self.client.get(hbase_table.encode(), get)
                for column in result.columnValues:
                    values[column.qualifier.decode('utf-8')] = column.value.decode('utf-8')
                return values
            except Exception as e:
                print(e)
                trash -= 1
                self.reconnect()

        if trash == 0:
            raise Exception('fuck hbase down')

    def put_result(self, hhase_row: str, hbase_item: dict, hbase_table: str, column_name: bytes = b"wa"):
        """
        存储
        :param hhase_row: rowkey
        :param hbase_item: 数据字典
        :param hbase_table: 表名
        :param column_name: 列簇
        :return:
        """
        trash = 5
        while trash != 0:
            try:
                coulumn_values = []
                rowkey = hhase_row.encode(encoding='utf-8')
                for key in hbase_item:
                    column = key.encode(encoding='utf-8')
                    value = str(hbase_item[key]).encode(encoding='utf-8')
                    coulumn_value = TColumnValue(column_name, column, value)
                    coulumn_values.append(coulumn_value)
                tput = TPut(rowkey, coulumn_values)
                self.client.put(hbase_table.encode(encoding='utf-8'), tput)
                return 'put success'
            except Exception as e:
                print(e)
                trash -= 1
                self.reconnect()

        if trash == 0:
            raise Exception('fuck hbase down')

    def delete_result(self, hbase_row: str, hbase_table: str):
        """
        删除
        :param hbase_row: rowkey
        :param hbase_table: 表名
        :return:
        """
        tdelete = TDelete(hbase_row.encode())
        self.client.deleteSingle(hbase_table.encode(), tdelete)

    def exists(self, hbase_row: str, hbase_table: str) -> bool:
        """
        rowkey是否存在
        :param hbase_row: rowkey
        :param hbase_table: 表名
        :return:
        """
        get = TGet()
        get.row = hbase_row.encode()
        result = self.client.exists(hbase_table.encode(), get)
        return result

    def scan_result(self, hbase_table: str, start_row: str = None):
        """
        扫表
        :param hbase_table: 表名
        :param start_row: 起始rowkey
        :return:
        """
        tscan = TScan(startRow=start_row)
        scan_id = self.client.openScanner(hbase_table.encode(), tscan)
        row_list = self.client.getScannerRows(scan_id, 1000)
        while row_list:
            for r in row_list:
                dict_data = {}
                hp = r.row
                dict_data['rowkey'] = hp.decode()
                for columnValue in r.columnValues:
                    try:
                        qualifier = columnValue.qualifier.decode()
                        value = columnValue.value.decode()
                        dict_data[qualifier] = value
                    except Exception as e:
                        print(e)
                        continue
                yield dict_data
            try:
                row_list = self.client.getScannerRows(scan_id, 10000)
            except Exception as e:
                print(e)
                self.reconnect()
                row_list = self.client.getScannerRows(scan_id, 10000)


if __name__ == '__main__':
    hbase_server = [('192.168.129.11', 9090), ('192.168.129.12', 9090)]
    hb_list = [HBaseClient(h[0], h[1], hbase_server)
               for h in hbase_server]

    def hb_gen_func():
        while True:
            for hb in hb_list:
                yield hb

    import time
    from multiprocessing import Process
    hb_gen = hb_gen_func()

    def run():
        while True:
            hb = next(hb_gen)
            print(hb.get_result("1918846875-HzLYraKvJ", "WEIBO_INFO_TABLE")['wb_harmful_type_graph_time'])
            time.sleep(random.random())
            print('-')

    # run()

    for i in range(5):
        p = Process(target=run)
        p.start()

