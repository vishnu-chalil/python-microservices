from configparser import ConfigParser
from kafka import KafkaProducer, KafkaConsumer
import json
import random

config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
client_config = dict(config_parser['kafka_client'])

cheese_producer = KafkaProducer(bootstrap_servers=config_parser['kafka_client']['bootstrap.servers'])

sauce_consumer = KafkaConsumer(bootstrap_servers=config_parser['kafka_client']['bootstrap.servers'])
sauce_consumer.subscribe(['pizza-with-sauce'])


def start_service():
    while True:
        for msg in sauce_consumer:
            if msg is None:
                pass
            # elif msg.error():
            #      pass
            else:
                pizza = json.loads(msg.value.decode ('utf-8'))
                add_cheese(pizza['order_id'], pizza)


def add_cheese(order_id, pizza):
    pizza['cheese'] = calc_cheese()
    print(pizza)
    print("-----")
    cheese_producer.send('pizza-with-cheese', json.dumps(pizza).encode ('utf-8'))


def calc_cheese():
    i = random.randint(0, 6)
    cheeses = ['extra', 'none', 'three cheese', 'goat cheese', 'extra', 'three cheese', 'goat cheese']
    return cheeses[i]


if __name__ == '__main__':
    start_service()
