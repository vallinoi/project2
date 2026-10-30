import os
import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import OperationalError

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://user:pass@merchant-db:5432/merchants")
# DB container may not be ready yet, so retry
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


class Merchant(Base):
    __tablename__ = "merchants"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    ssn = Column(String, nullable=False)
    email = Column(String, nullable=False)
    phone_number = Column(String, nullable=False)
    allows_discount = Column(Boolean, nullable=False)


Base.metadata.create_all(engine)

app = FastAPI()

class MerchantIn(BaseModel):
    name: str
    ssn: str
    email: EmailStr
    phoneNumber: str
    allowsDiscount: bool


@app.post("/merchants", status_code = 201)
def create_merchant(m: MerchantIn):
    with  Session() as db:
        merchant = Merchant(
            name=m.name,
            ssn=m.ssn,
            email=m.email,
            phone_number=m.phoneNumber,
            allows_discount=m.allowsDiscount,
        )
        db.add(merchant)
        db.commit()
        db.refresh(merchant)
        return merchant.id

@app.get("/merchants/{merchant_id}")
def get_merchant(merchant_id: int):
    with Session() as db:
        merchant = db.get(Merchant, merchant_id)
        if merchant is None:
            raise HTTPException(status_code=404, detail="Merchant does not exist")
        return {
            "name": merchant.name,
            "ssn": merchant.ssn,
            "email": merchant.email,
            "phoneNumber": merchant.phone_number,
            "allowsDiscount": merchant.allows_discount,
        }