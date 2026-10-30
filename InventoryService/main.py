import os
import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import OperationalError

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://user:pass@inventory-db:5432/inventory")

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
