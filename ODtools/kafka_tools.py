from kafka import KafkaConsumer, KafkaProducer
from ODtools.singleton_tools import Singleton


class KafkaConsumerClient(metaclass=Singleton):
    def __init__(self, *topic, group_id: str, client_id: str,
                 bootstrap_servers: list = None, auto_offset_reset: str = 'latest',
                 **kwargs):

        self.consumer = KafkaConsumer(*topic,
                                      bootstrap_servers=bootstrap_servers,
                                      group_id=group_id,
                                      client_id=client_id,
                                      auto_offset_reset=auto_offset_reset,
                                      **kwargs)


class KafkaProducerClient(metaclass=Singleton):
    def __init__(self, bootstrap_servers: list = None, retries: int = 5, acks: str = 'all', **kwargs):

        self.producer = KafkaProducer(acks=acks,
                                      retries=retries,
                                      bootstrap_servers=bootstrap_servers,
                                      **kwargs)


if __name__ == '__main__':
    pass
