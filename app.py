from flask import Flask, jsonify
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
from pybreaker import CircuitBreaker, CircuitBreakerError
from flask_cors import CORS
from pydantic import ValidationError
import time
from flasgger import Swagger

load_dotenv()

MAX_RETRIES = 3
RETRY_DELAY_BASE = 5  # seconds
USER_ENDPOINT = os.environ.get("USER_ENDPOINT")
TEMPLATE_ENDPOINT = os.environ.get("TEMPLATE_ENDPOINT")
RABBITMQ_URL = os.environ.get("RABBITMQ_HOST", 'amqp://guest:guest@localhost:5672/')
REDIS_HOST = os.environ.get("REDIS_HOST")
REDIS_PORT = os.environ.get("REDIS_PORT", 6379)
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")
PORT = int(os.environ.get("PORT", 5000))

app = Flask(__name__)
swagger = Swagger(app, template={
    "info": {
        "title": "Notification System API",
        "description": "Handles notification delivery and retry logic with idempotency and circuit breaker.",
        "version": "1.0.0"
    },
    "basePath": "/"
})
CORS(app)

breaker = CircuitBreaker(fail_max=5, reset_timeout=60, exclude=[ValueError])
r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    ssl=True,
    decode_responses=True
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === GLOBAL CONSUMER STATE ===
consumer_thread = None
consumer_active = False


# === CALLBACK: Process Message ===
def callback(ch, method, properties, body):
    """Process notification messages from RabbitMQ."""
    try:
        data = json.loads(body)
        payload = NotificationEvent(**data)
        request_id = payload.data.request_id
        link = payload.data.variables.link
        user_id = payload.data.user_id
        template_code = payload.data.template_code
        retry_key = f"{request_id}:retries"
        idempotency_key = f"{request_id}:processed"

        retries = int(r.get(retry_key) or 0)

        # === IDEMPOTENCY CHECK ===
        if r.exists(idempotency_key):
            logger.info(f"Duplicate request detected (request_id={request_id}). Skipping.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # === GET PUSH DATA (Cached or Fresh) ===
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
                r.set(user_id, json.dumps({"push_token": push_token, "user_name": user_name}), ex=3600)
            return push_token, template_data

        # === CIRCUIT BREAKER WRAPPED SEND ===
        @breaker
        def attempt():
            push_token, template_data = get_push_data()
            logger.info(f"Sending to token: {push_token[:20]}... link: {link}")
            send_notification(push_token, template_data, link)

        attempt()
        r.set(idempotency_key, 1, ex=3600 * 6)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        r.delete(retry_key)
        update_notifcation_status(request_id, "delivered")
        logger.info(f"Notification sent successfully! (request_id={request_id})")

    except CircuitBreakerError:
        logger.error("Circuit open: storing message for delayed retry.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    except InvalidArgumentError as e:
        logger.info(f"Invalid device token: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        ch.connection.channel().basic_publish(
            exchange="notifications.direct",
            routing_key="failed",
            body=body
        )
        update_notifcation_status(request_id, "failed")
        r.set(idempotency_key, 1, ex=3600 * 6)
        r.delete(retry_key)

    except Exception as e:
        logger.error(f"Send failed: {e}")
        retries += 1
        r.set(retry_key, retries, ex=3600)
        if retries >= MAX_RETRIES:
            logger.error(f"Max retries reached ({retries}) → moving to dead-letter")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            ch.connection.channel().basic_publish(
                exchange="notifications.direct",
                routing_key="failed",
                body=body
            )
            r.delete(retry_key)
        else:
            delay = RETRY_DELAY_BASE * (2 ** (retries - 1))
            logger.info(f"Retrying in {delay}s (retry {retries}/{MAX_RETRIES})")
            time.sleep(delay)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        update_notifcation_status(request_id, "failed")

    except ValidationError as e:
        logger.error(f"Invalid payload: {e.json()} → moving to dead-letter")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        ch.connection.channel().basic_publish(
            exchange="notifications.direct",
            routing_key="failed",
            body=body
        )
        update_notifcation_status(request_id, "failed")


# === CONSUMER LOOP (Non-blocking) ===
def consume_messages():
    global consumer_active
    while consumer_active:
        try:
            connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
            channel = connection.channel()
            channel.exchange_declare(exchange="notifications.direct", exchange_type=ExchangeType.direct, durable=True)
            channel.queue_declare(queue="push.queue", durable=True)
            channel.queue_declare(queue="failed.queue", durable=True)
            channel.queue_bind(exchange="notifications.direct", queue="push.queue", routing_key="push")
            channel.queue_bind(exchange="notifications.direct", queue="failed.queue", routing_key="failed")
            channel.basic_qos(prefetch_count=1)

            def on_message(ch, method, properties, body):
                callback(ch, method, properties, body)

            channel.basic_consume(queue="push.queue", on_message_callback=on_message, auto_ack=False)
            logger.info("Consumer started. Waiting for messages...")
            channel.start_consuming()

        except Exception as e:
            logger.error(f"Consumer crashed: {e}. Restarting in 5s...")
            time.sleep(5)


# === START CONSUMER ===
def start_consumer():
    global consumer_thread, consumer_active
    if consumer_thread is None or not consumer_thread.is_alive():
        consumer_active = True
        consumer_thread = threading.Thread(target=consume_messages, daemon=True)
        consumer_thread.start()
        logger.info("Consumer thread started.")


# === STOP CONSUMER ===
def stop_consumer():
    global consumer_active
    consumer_active = False
    if consumer_thread and consumer_thread.is_alive():
        consumer_thread.join(timeout=5)
    logger.info("Consumer stopped.")


# === HEALTH CHECK ===
@app.route("/health", methods=["GET"])
def health():
    """
    Health Check Endpoint
    ---
    tags:
      - Monitoring
    summary: Check service and consumer health
    responses:
      200:
        description: Service is running normally
        schema:
          type: object
          properties:
            status:
              type: string
            queue:
              type: string
            breaker_state:
              type: string
            consumer_running:
              type: boolean
    """
    return jsonify({
        'status': 'healthy',
        'queue': "push.queue",
        'breaker_state': breaker.current_state,
        'consumer_running': consumer_thread.is_alive() if consumer_thread else False
    }), 200


# === GRACEFUL SHUTDOWN ===
import atexit
atexit.register(stop_consumer)


# === MAIN ===
if __name__ == "__main__":
    start_consumer()  # Start in background
    try:
        app.run(host='0.0.0.0', port=5000, use_reloader=False)
    finally:
        stop_consumer()