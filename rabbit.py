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
  "pattern": "send_push_event",
  "data":{
  "notification_type": "push",
  "user_id": "c1e37847-4073-425e-a2a0-eafbf5b79098",
  "template_code": "REMINDER",
  "variables": {
    "name": "Alex Smith",
    "link": "https://example.com/view",
    "meta": {}
  },
  "request_id": "req-push-9169",
  "priority": 1,
  "metadata": {}
}
}

channel.basic_publish(exchange='notifications.direct', routing_key="push", body=json.dumps(message))
print(f"sent message {message}")
connection.close()