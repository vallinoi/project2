import os
import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import OperationalError

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://user:pass@buyer-db:5432/buyers")

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

class Buyer(Base):
    __tablename__ = "buyers"
    id = Column(Integer, primary_key = True, index = True)
    name = Column(String, nullable=False)
    ssn = Column(String, nullable=False)
    email = Column(String, nullable=False)
    phone_number = Column(String, nullable=False)

Base.metadata.create_all(engine)

app = FastAPI()


class BuyerIn(BaseModel):
    name: str
    ssn: str
    email: EmailStr
    phoneNumber: str


@app.post("/buyers", status_code = 201)
def create_buyer(b:BuyerIn):
    with Session() as db:
        buyer = Buyer(
                    name=b.name, 
                    ssn=b.ssn, 
                    email=b.email, 
                    phone_number=b.phoneNumber,)
        db.add(buyer)
        db.commit()
        db.refresh(buyer)
        return buyer.id 

@app.get("/buyers/{buyer_id}")
def get_buyer(buyer_id: int):
    with Session() as db:
        buyer = db.get(Buyer, buyer_id)
        if buyer is None:
            raise HTTPException(status_code=404, detail="Buyer does not exist")
        return {
            "name": buyer.name,
            "ssn": buyer.ssn,
            "email": buyer.email,
            "phoneNumber": buyer.phone_number,
        }
