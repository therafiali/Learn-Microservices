from fastapi import FastAPI
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import json
from .schema import Order
from contextlib import asynccontextmanager


async def consume(topic, broker):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=broker,
        group_id="my-group")
    await consumer.start()
    try:
        async for msg in consumer:
            print(f"{msg.topic} : {msg.value}")
    finally:
        await consumer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("life span is fire..................")
    task = asyncio.create_task(consume("order",'broker:19092'))
    yield
    

app = FastAPI(lifespan=lifespan)


@app.post("/order")
async def create_producer(order: Order):
    producer = AIOKafkaProducer(bootstrap_servers='broker:19092')
    orderJson = json.dumps(order.__dict__).encode("utf-8")
    await producer.start()
    try:
        await producer.send_and_wait("order", orderJson)
    finally:
        await producer.stop()
    return orderJson

