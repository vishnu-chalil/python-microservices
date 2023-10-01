from configparser import ConfigParser
from kafka import KafkaProducer, KafkaConsumer
import json
import random

config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
client_config = dict(config_parser['kafka_client'])

sauce_producer = KafkaProducer(bootstrap_servers=config_parser['kafka_client']['bootstrap.servers'])

pizza_consumer = KafkaConsumer(bootstrap_servers=config_parser['kafka_client']['bootstrap.servers'])
pizza_consumer.subscribe(['pizza'])




def calc_sauce():
    i = random.randint(0, 8)
    sauces = ['regular', 'light', 'extra', 'none', 'alfredo', 'regular', 'light', 'extra', 'alfredo']
    return sauces[i]


def add_sauce(order_id, pizza):
    pizza['sauce'] = calc_sauce()
    print(pizza)
    print("-----")
    sauce_producer.send('pizza-with-sauce', json.dumps(pizza).encode ('utf-8'))
    return None

def start_service():
    while True:
        for msg in pizza_consumer:
            if msg is None:
                pass
            # elif msg.error():
            #     pass
            else:
                pizza = json.loads(msg.value.decode ('utf-8'))
                add_sauce(pizza['order_id'], pizza)


if __name__ == '__main__':
    start_service()
