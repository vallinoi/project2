import os
import time
import json
import threading
import pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import OperationalError

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://user:pass@inventory-db:5432/inventory")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
EXCHANGE = "events"
QUEUE = "inventory.payment-result"


engine = None
for _ in range(15):
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        engine.connect().close()
        break
    except OperationalError:
        time.sleep(2)

Base = declarative_base()
Session = sessionmaker(bind = engine)

class Product(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key = True, index = True)
    merchant_id = Column(Integer, nullable = False)
    product_name = Column(String, nullable = False)
    price = Column(Float, nullable = False)
    quantity = Column(Integer, nullable = False)
    reserved = Column(Integer, nullable = False, default = 0)

Base.metadata.create_all(engine)

app = FastAPI()


class ProductIn(BaseModel):
    merchantId: int
    productName: str
    price: float
    quantity: int


@app.post("/products", status_code = 201)
def create_product(p:ProductIn):
    with Session() as db:
        product = Product(
                    merchant_id=p.merchantId,
                    product_name=p.productName,
                    price=p.price,
                    quantity=p.quantity,
                    reserved=0,)
        db.add(product)
        db.commit()
        db.refresh(product)
        return product.id 

@app.get("/products/{product_id}")
def get_product(product_id: int):
    with Session() as db:
        product = db.get(Product, product_id)
        if product is None:
            raise HTTPException(status_code=404, detail="Product does not exist")
        return {
            "merchantId": product.merchant_id,
            "productName": product.product_name,
            "price": product.price,
            "quantity": product.quantity,
            "reserved": product.reserved,
        }


# Internal endpoint: OrderService calls this to reserve one item
@app.post("/products/{product_id}/reserve")
def reserve_product(product_id: int):
    with Session() as db:
        # lock the row so two orders can't reserve the last item at once
        product = db.get(Product, product_id, with_for_update=True)
        if product is None:
            raise HTTPException(status_code=404, detail="Product does not exist")
        if product.quantity - product.reserved <= 0:
            raise HTTPException(status_code=400, detail="Product is sold out")
        product.reserved += 1
        db.commit()
        return {"reserved": product.reserved}

def handle_payment_event(ch, method, properties, body):
    event = json.loads(body)
    with Session() as db:
        product = db.get(Product, event["productId"], with_for_update=True)
        if product is not None and product.reserved > 0:
            product.reserved -= 1
            if method.routing_key == "payment.success":
                product.quantity -= 1
            db.commit()
    ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_payment_events():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
            channel.queue_declare(queue=QUEUE, durable=True)
            channel.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key="payment.success")
            channel.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key="payment.failure")
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=QUEUE, on_message_callback=handle_payment_event)
            channel.start_consuming()
        except pika.exceptions.AMQPError:
            time.sleep(2)


@app.on_event("startup")
def start_consumer():
    threading.Thread(target=consume_payment_events, daemon=True).start()