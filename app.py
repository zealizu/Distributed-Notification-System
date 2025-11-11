from flask import Flask
from models import NotificationPayload
from notify import send_notification
import pika
from pika.exchange_type import ExchangeType
import redis
import os 
import json
from dotenv import load_dotenv
import requests
import logging
from firebase_admin.exceptions import InvalidArgumentError
import threading
from pybreaker import CircuitBreaker, CircuitBreakerError
from flask_cors import CORS
from pydantic import ValidationError
import time

MAX_RETRIES = 5
RETRY_DELAY_BASE = 5  # seconds
app = Flask(__name__)
CORS(app)
breaker = CircuitBreaker(fail_max=5, reset_timeout=60, exclude=[ValueError])
r = redis.Redis(host="localhost", port=55006, db=0)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        payload = NotificationPayload(**data)
        # print("Valid notification received:", payload.model_dump())
    except ValidationError as e:
        print("Invalid payload:", e.json())
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    request_id = payload.request_id
    retry_key = f"{request_id}:retries"
    retries = int(r.get(retry_key) or 0)
    try:
        @breaker
        def attempt():
            send_notification()
        attempt()
        ch.basic_ack(delivery_tag=method.delivery_tag)
        r.delete(retry_key)
        print("Notification sent successfully!")
    except CircuitBreakerError:
        print("Circuit open: storing message for delayed retry.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)  # remove from queue
    except InvalidArgumentError:
        print(f"Invalid device token")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        r.delete(retry_key)
    except Exception as e:
        print(e)
        retries += 1
        r.set(retry_key, retries, ex=3600)  # keep retry count for 1h

        if retries >= MAX_RETRIES:
            print(f"Max retries reached ({retries}) → moving to dead-letter queue")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            r.delete(retry_key)
        else:
            delay = RETRY_DELAY_BASE * (2 ** (retries - 1))
            print(f"Send failed, retrying in {delay}s (retry {retries}/{MAX_RETRIES})")
            time.sleep(delay)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def start_consumer():
    """Start pika consumer in a thread."""
    def connect_and_consume():
        while True:
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
                channel = connection.channel()
                channel.exchange_declare(exchange="notification.direct", exchange_type=ExchangeType.direct)
                failed_queue = channel.queue_declare(queue="failed.queue")
                push_queue = channel.queue_declare(queue="push.queue", arguments={
                        'x-dead-letter-exchange': 'notification.direct',
                        'x-dead-letter-routing-key': 'failed'
                    })
                channel.queue_bind(exchange="notification.direct", queue=push_queue.method.queue, routing_key="push")
                channel.queue_bind(exchange="notification.direct", queue=failed_queue.method.queue, routing_key="failed")
                channel.basic_qos(prefetch_count=1) #this means that each consumer will only process one message at a time
                channel.basic_consume(queue="push.queue", on_message_callback=callback)
                logger.info('Push consumer started. Waiting for messages...')
                channel.start_consuming()

                logger.info('Push consumer started. Waiting for messages...')
                channel.start_consuming()  # Blocks forever
            except Exception as e:
                logger.error(f"Consumer error: {e}. Reconnecting in 5s...")
                time.sleep(5)

    consumer_thread = threading.Thread(target=connect_and_consume, daemon=True)
    consumer_thread.start()

@app.route("/health", methods=["GET"])
def health():
    return {'status': 'healthy', 'queue': "push.queue", 'breaker_state': breaker.current_state}, 200

if __name__ == "__main__":
    start_consumer()
    app.run(debug=True)