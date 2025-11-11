import pika
from pika.exchange_type import ExchangeType
import random
import time

def on_message_received(ch, method, properties, body):
    # processing_time = random.randint(1,6)
    # print(f"received new message:{body}, will take {processing_time} to process")
    # time.sleep(processing_time)
    # ch.basic_ack(delivery_tag= method.delivery_tag)
    print("Finished processing the notification ")

    

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()
channel.exchange_declare(exchange="routing", exchange_type=ExchangeType.direct)
queue = channel.queue_declare(queue="notification", exclusive=True)
channel.queue_bind(exchange="routing", queue=queue.method.queue, routing_key="notification")

channel.basic_qos(prefetch_count=1) #this means that each consumer will only process one message at a time

channel.basic_consume(queue="notification", auto_ack=True, on_message_callback=on_message_received)

print("Starting Consuming")

channel.start_consuming()