import asyncio
from fastapi import FastAPI
from kafka_producer import start_producer, stop_producer
from kafka_consumer import consume_messages
from kafka_admin import create_topic_if_not_exist
import produce
from contextlib import asynccontextmanager

TOPICS = ["my-topic1", "my-topic2","new-topic","sreeshma"]
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Asynchronous context manager for the background Kafka consumer thread.
    """
    await start_producer(TOPICS)
    consumer_task = asyncio.create_task(consume_messages(TOPICS))  # Create task for consumption

    try:
        yield  # Code to be executed before the consumer starts
    finally:
        await stop_producer()
        consumer_task.cancel()  # Cancel the consumer task when the context exits
        await consumer_task  # Wait for the task to finish cancelling

app = FastAPI(lifespan=lifespan, title="My API",
    description="This is a sample FastAPI application",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc")

app.include_router(produce.router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
