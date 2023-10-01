import json

from pizza import Pizza, PizzaOrder
from configparser import ConfigParser
from kafka import KafkaConsumer

config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
producer_config = dict(config_parser['kafka_client'])
consumer_config = dict(config_parser['kafka_client'])
consumer_config.update(config_parser['consumer'])

cheeses = {}
cheese_topic = 'pizza-with-cheese'

def start_consumer():
    cheese_consumer = KafkaConsumer(bootstrap_servers=config_parser['kafka_client']['bootstrap.servers'])
    cheese_consumer.subscribe([cheese_topic])
    while True:
        for event in cheese_consumer:
            if event is None:
                pass
            else:
                pizza = json.loads(event.value.decode ('utf-8'))
                add_cheese_count(pizza['cheese'])
            
def add_cheese_count(cheese):
    if cheese in cheeses:
        cheeses[cheese] = cheeses[cheese] + 1
    else:
        cheeses[cheese] = 1

def generate_report():
    return json.dumps(cheeses, indent = 4)     

