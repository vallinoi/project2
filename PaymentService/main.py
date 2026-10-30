import os
import time
import json
import pika
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import OperationalError


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://user:pass@payment-db:5432/payments")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
EXCHANGE = "events"
QUEUE = "payment.order-created"

engine = None
for _ in range(15):
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        engine.connect().close()
        break
    except OperationalError:
        time.sleep(2)

Base = declarative_base()
Session = sessionmaker(bind=engine)


class Payment(Base):
    __tablename__ = "payments"
    id = Column(Integer, primary_key = True, index = True)
    order_id = Column(Integer, nullable = False)
    result = Column(String, nullable = False)

Base.metadata.create_all(engine)

def luhn_valid(number:str) -> bool:
    if not number.isdigit():
        return False
    total = 0
    for i, ch in enumerate(reversed(number)):
        d = int(ch)
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def card_valid(card: dict) -> bool:
    number = str(card.get("cardNumber", ""))
    month = card.get("expirationMonth")
    year = card.get("expirationYear")
    cvc = card.get("cvc")
    return (
        luhn_valid(number)
        and isinstance(month, int) and 1 <= month <= 12
        and isinstance(year, int) and 1000 <= year <= 9999
        and isinstance(cvc, int) and 100 <= cvc <= 999
    )


def on_order_created(ch, method, properties, body):
    order = json.loads(body)
    result = "success" if card_valid(order["creditCard"]) else "failure"

    with Session() as db:
        db.add(Payment(order_id=order["orderId"], result = result))
        db.commit()

    # forward the order info but the card details
    event = {k: v for k, v in order.items() if k != "creditCard"}
    ch.basic_publish(
        exchange = EXCHANGE,
        routing_key = f"payment.{result}",
        body = json.dumps(event),
        properties = pika.BasicProperties(delivery_mode = 2, content_type = "application/json"),
    )
    ch.basic_ack(delivery_tag = method.delivery_tag)
    print(f"Order {order['orderId']}: payment {result}", flush = True)


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
    channel.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key="order.created")
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE, on_message_callback=on_order_created)
    print("PaymentService waiting for order.created events", flush=True)
    channel.start_consuming()

main()
