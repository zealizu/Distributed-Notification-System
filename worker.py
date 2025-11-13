from models import NotificationEvent, APIResponse
from notify import send_notification, update_notifcation_status
from utils import get_template_data
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
from pybreaker import CircuitBreaker, CircuitBreakerError,CircuitBreakerListener
from pydantic import ValidationError
import time

load_dotenv()


class RedisBreakerListener(CircuitBreakerListener):
    def __init__(self, redis_client):
        self.redis = redis_client

    def state_change(self, cb, old_state, new_state):
        print(f"Breaker changed from {old_state.name} → {new_state.name}")
        self.redis.set("breaker_state", new_state.name)

MAX_RETRIES = 3
RETRY_DELAY_BASE = 5  # seconds
USER_ENDPOINT = os.environ.get("USER_ENDPOINT")
TEMPLATE_ENDPOINT = os.environ.get("TEMPLATE_ENDPOINT")
RABBITMQ_URL = os.environ.get("RABBITMQ_HOST", 'localhost')
REDIS_HOST = os.environ.get("REDIS_HOST")
REDIS_PORT = os.environ.get("REDIS_PORT")
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT,password=REDIS_PASSWORD, ssl=True, decode_responses=True)
breaker = CircuitBreaker(fail_max=5, reset_timeout=60,  listeners=[RedisBreakerListener(r)])
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
r.set("breaker_state", "closed")

def callback(ch, method, properties, body):
    """Process notification messages from RabbitMQ."""
    try:
        data = json.loads(body)
        payload = NotificationEvent(**data)
        # print("Valid notification received:", payload.model_dump())
        request_id = payload.data.request_id
        link = payload.data.variables.link
        user_id = payload.data.user_id
        template_code = payload.data.template_code
        retry_key = f"{request_id}:retries"
        idempotency_key = f"{request_id}:processed"
        retries = int(r.get(retry_key) or 0)
        try:
            if r.exists(idempotency_key):
                logger.info(f"Duplicate request detected (request_id={request_id}). Skipping processing.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            def get_push_data():
                if r.exists(user_id):
                    user_payload = json.loads(r.get(user_id))
                    push_token = user_payload["push_token"]
                    user_name = user_payload["user_name"]
                    template_data = get_template_data(template_code, user_name)
                else:
                    user_response = requests.get(f"{USER_ENDPOINT}/{user_id}", timeout=10)
                    user_response.raise_for_status()
                    user_response_data = user_response.json()
                    user_payload = APIResponse(**user_response_data)
                    push_token = user_payload.data.push_token
                    user_name = user_payload.data.name
                    template_data = get_template_data(template_code, user_name)
                r.set(user_id, json.dumps({"push_token": push_token, "user_name":user_name}), ex=3600)
                return push_token, template_data
            try:
                @breaker
                def attempt():
                    push_token, template_data = get_push_data()
                    logger.info(link)
                    send_notification(push_token,template_data, link)
                attempt()
                
                r.set(idempotency_key, 1, ex=3600 * 6)  # keep record for 6 hours
                ch.basic_ack(delivery_tag=method.delivery_tag)
                r.delete(retry_key)
                update_notifcation_status(request_id, "delivered")
                logger.info("Notification sent successfully!")
            except CircuitBreakerError:
                logger.error("Circuit open: storing message for delayed retry.")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)  # remove from queue
            except InvalidArgumentError:
                logger.info(f"Invalid device token")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                ch.connection.channel().basic_publish(exchange = "notifications.direct", routing_key="failed", body=body)
                update_notifcation_status(request_id, "failed")
                r.set(idempotency_key, 1, ex=3600 * 6)  # keep record for 6 hours
                r.delete(retry_key)            
        except Exception as e:
            logger.error(e)
            retries += 1
            r.set(retry_key, retries, ex=3600)  # keep retry count for 1h
            if retries >= MAX_RETRIES:
                logger.error(f"Max retries reached ({retries}) → moving to dead-letter queue")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                ch.connection.channel().basic_publish(exchange = "notifications.direct", routing_key="failed", body=body)
                r.delete(retry_key)
            else:
                delay = RETRY_DELAY_BASE * (2 ** (retries - 1))
                logger.error(f"Send failed, retrying in {delay}s (retry {retries}/{MAX_RETRIES})")
                time.sleep(delay)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                update_notifcation_status(request_id, "failed")
    except ValidationError as e:
        logger.error(f"Invalid payload:, {e.json()} moving to dead-letter queue")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        ch.connection.channel().basic_publish(exchange = "notifications.direct", routing_key="failed", body=body)
        update_notifcation_status(request_id, "failed")
        

def start_consumer():
    """Start pika consumer in a thread."""
    def connect_and_consume():
        while True:
            try:
                connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
                channel = connection.channel()
                channel.exchange_declare(exchange="notifications.direct", exchange_type=ExchangeType.direct, durable=True)
                failed_queue = channel.queue_declare(queue="failed.queue", durable=True)
                push_queue = channel.queue_declare(queue="push.queue",durable=True)
                channel.queue_bind(exchange="notifications.direct", queue=push_queue.method.queue, routing_key="push")
                channel.queue_bind(exchange="notifications.direct", queue=failed_queue.method.queue, routing_key="failed")
                channel.basic_qos(prefetch_count=1) #this means that each consumer will only process one message at a time
                channel.basic_consume(queue="push.queue", on_message_callback=callback)
                logger.info('Push consumer started. Waiting for messages...')
                channel.start_consuming()
            except Exception as e:
                logger.error(f"Consumer error: {e}. Reconnecting in 5s...")
                time.sleep(5)

    consumer_thread = threading.Thread(target=connect_and_consume, daemon=True)
    consumer_thread.start()
     


if __name__ == "__main__":
    start_consumer()
    while True:
        time.sleep(60)