import os
import json
import time
import pika
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
SENDER = os.getenv("SENDGRID_SENDER_EMAIL")
EXCHANGE = "events"
QUEUE = "email.events"  # ONE shared queue, so instances take turns


def send_email(to: str, subject: str, body: str):
    message = Mail(
        from_email=SENDER,
        to_emails=to,
        subject=subject,
        plain_text_content=body,
    )
    SendGridAPIClient(SENDGRID_API_KEY).send(message)


def on_event(ch, method, properties, body):
    event = json.loads(body)
    order_id = event["orderId"]

    if method.routing_key == "order.created":
        subject = "Order has been created"
        text = (
            f"Order {order_id} has been created. "
            f"Product: {event['productName']}. Price: {event['totalPrice']}"
        )
    elif method.routing_key == "payment.success":
        subject = "Order has been purchased"
        text = f"Order {order_id} has been successfully purchased"
    else:  # payment.failure
        subject = "Order purchase failed"
        text = f"Order {order_id} purchase has failed"

    for to in [event["buyerEmail"], event["merchantEmail"]]:
        try:
            send_email(to, subject, text)
            print(f"Sent '{subject}' for order {order_id} to {to}", flush=True)
        except Exception as e:
            print(f"Failed to email {to}: {e}", flush=True)

    # ack even if sending failed, so a bad address doesn't loop forever
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = None
    for _ in range(15):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            break
        except pika.exceptions.AMQPConnectionError:
            time.sleep(2)

    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(queue=QUEUE, durable=True)
    for key in ["order.created", "payment.success", "payment.failure"]:
        channel.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=key)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE, on_message_callback=on_event)
    print("EmailService waiting for events", flush=True)
    channel.start_consuming()


main()