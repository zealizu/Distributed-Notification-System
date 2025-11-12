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

channel.exchange_declare(exchange="notification.direct", exchange_type=ExchangeType.direct)


message ={
  "notification_type": "push",
  "user_id": "3d2f7a90-ff3d-4d6b-a492-90529a7e8b4d",
  "template_code": "welcome_push_v1",
  "variables": {
    "name": "Chidubem",
    "link": "https://wowmart.com/welcome",
    "meta": {
      "referral": "bonus_campaign"
    }
  },
  "request_id": "req_test_001",
  "priority": 5,
  "metadata": {
    "source": "onboarding"
  },
  "rich_media": {
    "icon": "https://cdn.wowmart.com/icons/app-icon.png",
    "image": "https://cdn.wowmart.com/banners/welcome.png",
    "action_link": "https://wowmart.com/open"
  }
}

channel.basic_publish(exchange='notification.direct', routing_key="push", body=json.dumps(message))
print(f"sent message {message}")
connection.close()