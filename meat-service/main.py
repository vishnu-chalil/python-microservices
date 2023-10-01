from configparser import ConfigParser
from kafka import KafkaProducer, KafkaConsumer
import json
import random

config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
client_config = dict(config_parser['kafka_client'])

meats_producer = KafkaProducer(bootstrap_servers=config_parser['kafka_client']['bootstrap.servers'])

cheese_consumer = KafkaConsumer(bootstrap_servers=config_parser['kafka_client']['bootstrap.servers'])
cheese_consumer.subscribe(['pizza-with-cheese'])


def start_service():
    while True:
        for msg in cheese_consumer:
            if msg is None:
                pass
            # elif msg.error():
            #     pass
            else:
                pizza = json.loads(msg.value.decode ('utf-8'))
                add_meats(pizza['order_id'], pizza)


def add_meats(order_id, pizza):
    pizza['meats'] = calc_meats()
    print(pizza)
    print("-----")
    meats_producer.send('pizza-with-meats',json.dumps(pizza).encode ('utf-8'))


def calc_meats():
    i = random.randint(0, 4)
    meats = ['pepperoni', 'sausage', 'ham', 'anchovies', 'salami', 'bacon', 'pepperoni', 'sausage', 'ham', 'anchovies', 'salami', 'bacon']
    selection = []
    if i == 0:
        return 'none'
    else:
        for n in range(i):
            selection.append(meats[random.randint(0, 11)])
    return ' & '.join(set(selection))


if __name__ == '__main__':
    start_service()
