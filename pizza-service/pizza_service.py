import json
import os
from pizza import Pizza, PizzaOrder
from configparser import ConfigParser
from kafka import KafkaProducer, KafkaConsumer

config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
producer_config = dict(config_parser['kafka_client'])
consumer_config = dict(config_parser['kafka_client'])
consumer_config.update(config_parser['consumer'])
try:
    broker=os.environ[config_parser['kafka_client']['bootstrap.servers']]
except Exception as e:
    print(e)
print(broker)
try:
    pizza_producer = KafkaProducer(bootstrap_servers=broker)
except Exception as e:
    print(e)
    exit(0)

pizza_warmer = {}

def order_pizzas(count):
    order = PizzaOrder(count)
    pizza_warmer[order.id] = order
    for i in range(count):
        new_pizza = Pizza()
        new_pizza.order_id = order.id
        pizza_producer.send('pizza', new_pizza.toJSON().encode ('utf-8'))
    pizza_producer.flush()
    return order.id

def get_order(order_id):
    order = pizza_warmer[order_id]
    print(order)
    print(pizza_warmer)
    if order == None:
        return "Order not found, perhaps it's not ready yet."
    else:
        return order.toJSON()


def load_orders():
    pizza_consumer = KafkaConsumer(bootstrap_servers=broker)
    pizza_consumer.subscribe(['pizza-with-veggies'])
    while True:
        for event in pizza_consumer:
            print(event)
            if event is None:
                pass
            # elif event.error():
            #     print(f'Bummer - {event.error()}')
            else:
                pizza = json.loads(event.value.decode ('utf-8'))
                print("pizza")
                print(pizza)
                add_pizza(pizza['order_id'], pizza)

def add_pizza(order_id, pizza):
    if order_id in pizza_warmer.keys():
        order = pizza_warmer[order_id]
        order.add_pizza(pizza)
