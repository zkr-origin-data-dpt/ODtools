import random
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from ODtools.hbase_client import *
from ODtools.hbase_client.ttypes import *
from ODtools.singleton_tools import Singleton


class HBaseClient(metaclass=Singleton):
    """
    HBase tools client
    """
    def __init__(self, hbase_address: str, hbase_port: int, hbase_servers: list = None):
        """
        :param hbase_address: hbase address
        :param hbase_port: hbase port
        :param hbase_servers: hbase server node list
        """
        self.address = hbase_address
        self.port = hbase_port
        self.servers = hbase_servers
        self.client = self.init_client(self.address, self.port)

    def reconnect(self):
        """
        reconnect hbase
        :return:
        """
        if self.servers:
            h_a, h_p = random.choice(self.servers)
            self.client = self.init_client(h_a, h_p)
        else:
            self.client = self.init_client(self.address, self.port)

    @staticmethod
    def init_client(address: str, port: int):
        """
        init hbase client
        :param address: hbase address
        :param port: hbase port
        :return: hbase client
        """
        transport = TSocket.TSocket(address, port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        transport.open()
        client = THBaseService.Client(protocol)
        return client

    def get_result(self, hbase_row: str, hbase_table: str) -> dict:
        """
        retrieve hbase data
        :param hbase_row: rowkey
        :param hbase_table: hbase table
        :return: result
        """
        trash = 5
        for i in range(trash):
            try:
                values = {}
                get = TGet()
                get.row = hbase_row.encode()
                result = self.client.get(hbase_table.encode(), get)
                for column in result.columnValues:
                    values[column.qualifier.decode('utf-8')] = column.value.decode('utf-8')
                return values
            except Exception as e:
                if i != trash - 1:
                    self.reconnect()
                    continue
                else:
                    raise e

    def put_result(self, hbase_row: str, hbase_item: dict, hbase_table: str, column_name: str = "wa") -> str:
        """
        create hbase data
        :param hbase_row: rowkey
        :param hbase_item: data
        :param hbase_table: hbase table
        :param column_name: column name
        :return:
        """
        if type(column_name) == str:
            column_name = column_name.encode(encoding='utf-8')
        if type(column_name) != bytes:
            raise Exception('Parameter error! column_name must is str or bytes')

        trash = 5
        for i in range(trash):
            try:
                coulumn_values = []
                rowkey = hbase_row.encode(encoding='utf-8')
                for key in hbase_item:
                    column = key.encode(encoding='utf-8')
                    value = str(hbase_item[key]).encode(encoding='utf-8')
                    coulumn_value = TColumnValue(column_name, column, value)
                    coulumn_values.append(coulumn_value)
                tput = TPut(rowkey, coulumn_values)
                self.client.put(hbase_table.encode(encoding='utf-8'), tput)
                return 'put success'
            except Exception as e:
                if i != trash - 1:
                    self.reconnect()
                    continue
                else:
                    raise e

    def delete_result(self, hbase_row: str, hbase_table: str):
        """
        delete hbase data
        :param hbase_row: rowkey
        :param hbase_table: hbase table
        :return:
        """
        tdelete = TDelete(hbase_row.encode())
        self.client.deleteSingle(hbase_table.encode(), tdelete)

    def exists(self, hbase_row: str, hbase_table: str) -> bool:
        """
        exists rowkey
        :param hbase_row: rowkey
        :param hbase_table: hbase table
        :return:
        """
        get = TGet()
        get.row = hbase_row.encode()
        result = self.client.exists(hbase_table.encode(), get)
        return result

    def scan_result(self, hbase_table: str, start_row: str = None):
        """
        scan hbase table data
        :param hbase_table: hbase table
        :param start_row: start rowkey
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
    pass
