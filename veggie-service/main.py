from configparser import ConfigParser
from kafka import KafkaProducer, KafkaConsumer
import json
import random

config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
client_config = dict(config_parser['kafka_client'])

#veggies_producer = Producer(client_config)
veggies_producer = KafkaProducer(bootstrap_servers=config_parser['kafka_client']['bootstrap.servers'])


meats_consumer = KafkaConsumer(bootstrap_servers=config_parser['kafka_client']['bootstrap.servers'])
meats_consumer.subscribe(['pizza-with-meats'])


def start_service():
    while True:
        for msg in meats_consumer:
            if msg is None:
                pass
            # elif msg.error():
            #     pass
            else:
                pizza = json.loads(msg.value.decode ('utf-8'))
                add_veggies(pizza['order_id'], pizza)


def add_veggies(order_id, pizza):
    pizza['veggies'] = calc_veggies()
    print(pizza)
    print("-----")
    veggies_producer.send('pizza-with-veggies', json.dumps(pizza).encode('utf-8'))


def calc_veggies():
    i = random.randint(0,4)
    veggies = ['tomato', 'olives', 'onions', 'peppers', 'pineapple', 'mushrooms', 'tomato', 'olives', 'onions', 'peppers', 'pineapple', 'mushrooms']
    selection = []
    if i == 0:
        return 'none'
    else:
        for n in range(i):
            selection.append(veggies[random.randint(0, 11)])
    return ' & '.join(set(selection))


if __name__ == '__main__':
    start_service()
