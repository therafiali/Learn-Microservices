from fastapi import FastAPI
from aiokafka import AIOKafkaProducer
import asyncio
import json
from sqlmodel import Field, Session, SQLModel, create_engine, select
from aiokafka import AIOKafkaConsumer


app = FastAPI()

class Order(SQLModel):
    id: int | None = Field(default=None)
    product: str = Field(index=True)
    product_id: int | None = Field(default=None, index=True)
    

@app.post("/order")
async def create_producer(order:Order):
    producer = AIOKafkaProducer(bootstrap_servers='broker:19092')
    orderJson = json.dumps(order.__dict__).encode("utf-8")
    await producer.start()
    try:
        # Produce message
        await producer.send_and_wait("order", orderJson)
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()
    return orderJson        

@app.get("/consumer")
async def consume():
    consumer = AIOKafkaConsumer(
        'order',
        bootstrap_servers='broker:19092',
        group_id="my-group")
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            print("consumed: ", msg.topic, msg.partition, msg.offset,
                  msg.key, msg.value, msg.timestamp)
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()
    return {"data":consumer}        