import os
import time
import json
import pika
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import OperationalError

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://user:pass@order-db:5432/orders")
MERCHANT_URL = os.getenv("MERCHANT_URL", "http://merchant-service:8001")
BUYER_URL = os.getenv("BUYER_URL", "http://buyer-service:8002")
INVENTORY_URL = os.getenv("INVENTORY_URL", "http://inventory-service:8003")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
EXCHANGE = "events"


def publish_event(routing_key: str, body: dict):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=routing_key,
        body=json.dumps(body),
        properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
    )
    connection.close()


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


class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, nullable=False)
    merchant_id = Column(Integer, nullable=False)
    buyer_id = Column(Integer, nullable=False)
    card_number = Column(String, nullable=False)
    discount = Column(Float, nullable=False, default=0)
    total_price = Column(Float, nullable=False)


Base.metadata.create_all(engine)

app = FastAPI()

class CreditCard(BaseModel):
    cardNumber: str
    expirationMonth: int
    expirationYear: int
    cvc: int

class OrderIn(BaseModel):
    productId: int
    merchantId: int
    buyerId: int
    creditCard: CreditCard
    discount: Optional[float] = None

def mask(card_number: str) -> str:
    return "*" * (len(card_number) - 4) + card_number[-4:]


@app.post("/orders", status_code = 201)
def create_order(o: OrderIn):
    # 1. merchant
    r = httpx.get(f"{MERCHANT_URL}/merchants/{o.merchantId}", timeout = 5)
    if r.status_code == 404:
        raise HTTPException(status_code = 400, detail = "Merchant does not exist")
    merchant = r.json()

    # 2. buyer
    r = httpx.get(f"{BUYER_URL}/buyers/{o.buyerId}", timeout=5)
    if r.status_code == 404:
        raise HTTPException(status_code=400, detail="Buyer does not exist")
    buyer = r.json()

    # 3. product
    r = httpx.get(f"{INVENTORY_URL}/products/{o.productId}", timeout=5)
    if r.status_code == 404:
        raise HTTPException(status_code=400, detail="Product does not exist")
    product = r.json()

    # 4. product belongs to merchant
    if product["merchantId"] != o.merchantId:
        raise HTTPException(status_code=400, detail="Product does not belong to merchant")

    # 5. discount
    discount = o.discount or 0
    if discount != 0 and not merchant["allowsDiscount"]:
        raise HTTPException(status_code=400, detail="Merchant does not allow discount")

    # 6. reserve
    r = httpx.post(f"{INVENTORY_URL}/products/{o.productId}/reserve", timeout=5)
    if r.status_code == 400:
        raise HTTPException(status_code=400, detail="Product is sold out")
    if r.status_code == 404:
        raise HTTPException(status_code=400, detail="Product does not exist")

    total = product["price"] * (1 - discount)

    with Session() as db:
        order = Order(
            product_id=o.productId,
            merchant_id=o.merchantId,
            buyer_id=o.buyerId,
            card_number=mask(o.creditCard.cardNumber),
            discount=discount,
            total_price=total,
        )
        db.add(order)
        db.commit()
        db.refresh(order)
        order_id = order.id

    publish_event("order.created", {
        "orderId": order_id,
        "productId": o.productId,
        "productName": product["productName"],
        "merchantId": o.merchantId,
        "merchantEmail": merchant["email"],
        "buyerId": o.buyerId,
        "buyerEmail": buyer["email"],
        "totalPrice": total,
        "creditCard": o.creditCard.model_dump(),
    })
    return order_id


@app.get("/orders/{order_id}")
def get_order(order_id: int):
    with Session() as db:
        order = db.get(Order, order_id)
        if order is None:
            raise HTTPException(status_code=404, detail="Order does not exist")
        return {
            "productId": order.product_id,
            "merchantId": order.merchant_id,
            "buyerId": order.buyer_id,
            "cardNumber": order.card_number,
            "totalPrice": order.total_price,
        }
