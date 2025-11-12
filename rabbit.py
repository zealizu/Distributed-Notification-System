import pika 
from pika.exchange_type import ExchangeType
import random
import time
import json
import os
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_URL = os.environ["RABBITMQ_HOST"]

connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
channel = connection.channel()

channel.exchange_declare(exchange="notifications.direct", exchange_type=ExchangeType.direct, durable=True)
channel.queue_bind(queue="push.queue", exchange="notifications.direct", routing_key="push")


message ={
  "notification_type": "push",
  "user_id": "ce958ef2-471c-4b2b-bb6e-0be8eae5e2ba",
  "template_code": "PRODUCT_UPDATE",
  "variables": {
    "name": "Alex Smith",
    "link": "https://example.com/view",
    "meta": {}
  },
  "request_id": "req-push-67892",
  "priority": 1,
  "metadata": {}
}

channel.basic_publish(exchange='notifications.direct', routing_key="push", body=json.dumps(message))
print(f"sent message {message}")
connection.close()