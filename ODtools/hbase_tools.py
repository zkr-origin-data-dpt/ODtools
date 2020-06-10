import random
import time

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

    def init_client(self, address: str, port: int):
        """
        init hbase client
        :param address: hbase address
        :param port: hbase port
        :return: hbase client
        """
        for i in range(1000):
            try:
                self.transport = TSocket.TSocket(address, port)
                self.transport = TTransport.TBufferedTransport(self.transport)
                protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
                self.transport.open()
                client = THBaseService.Client(protocol)
                return client
            except BaseException as e:
                if self.servers:
                    address, port = random.choice(self.servers)
                else:
                    raise Exception
        else:
            raise  Exception

    def get_result(self, hbase_row: str, hbase_table: str) -> dict:
        """
        retrieve hbase data
        :param hbase_row: rowkey
        :param hbase_table: hbase table
        :return: result
        """
        trash = 5
        self.reconnect()
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
            finally:
                self.close()

    def put_result(self, hbase_row: str, hbase_item: dict, hbase_table: str, column_name: str = "wa") -> str:
        """
        create hbase data
        :param hbase_row: rowkey
        :param hbase_item: data
        :param hbase_table: hbase table
        :param column_name: column name
        :return:
        """
        self.reconnect()
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
            finally:
                self.close()

    def delete_result(self, hbase_row: str, hbase_table: str):
        """
        delete hbase data
        :param hbase_row: rowkey
        :param hbase_table: hbase table
        :return:
        """
        trash = 5
        self.reconnect()
        for i in range(trash):
            try:
                tdelete = TDelete(hbase_row.encode())
                self.client.deleteSingle(hbase_table.encode(), tdelete)
            except BaseException as e:
                if i != trash - 1:
                    self.reconnect()
                    continue
                else:
                    raise e
            finally:
                self.close()

    def exists(self, hbase_row: str, hbase_table: str) -> bool:
        """
        exists rowkey
        :param hbase_row: rowkey
        :param hbase_table: hbase table
        :return:
        """
        trash = 5
        self.reconnect()
        for i in range(trash):
            try:
                get = TGet()
                get.row = hbase_row.encode()
                result = self.client.exists(hbase_table.encode(), get)
                return result
            except Exception as e:
                if i != trash - 1:
                    self.reconnect()
                    continue
                else:
                    raise e
            finally:
                self.close()

    def ping(self):
        """
        test hbase node
        :return:
        """
        self.init_client(self.address, self.port)
        try:
            return self.transport.isOpen()
        except BaseException as e:
            return False
        finally:
            self.close()

    def scan_result(self, hbase_table: str, start_row: str = None):
        """
        scan hbase table data
        :param hbase_table: hbase table
        :param start_row: start rowkey
        :return:
        """
        self.reconnect()
        tscan = TScan(startRow=start_row.encode() if start_row else None)
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
                row_list = self.client.getScannerRows(scan_id, 1000)
            except Exception as e:
                print(e)
                self.reconnect()
                row_list = self.client.getScannerRows(scan_id, 1000)
            finally:
                self.close()

    def close(self):
        """Close the underyling transport to the HBase instance.

        This method closes the underlying Thrift transport (TCP connection).
        """
        if not self.transport.isOpen():
            return
        self.transport.close()
        del self.transport

if __name__ == '__main__':
    pass
