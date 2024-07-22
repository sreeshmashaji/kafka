from aiokafka import AIOKafkaProducer
from kafka_admin import create_topic_if_not_exist

producer = AIOKafkaProducer(
    bootstrap_servers="localhost:9092",  # Replace with your server details
)

async def produce_message(topic: str, key:str,message: str):
    # Convert message to bytes (optional, depending on Kafka configuration)
    message_bytes = message.encode()
    key_bytes=key.encode()
    await producer.send_and_wait(topic, message_bytes,key_bytes)
    return {"message": f"Produced message to topic: {topic} with key: {key}"}

async def start_producer(topics: list):
    # Ensure the topics exist before starting the producer
    create_topic_if_not_exist(topics)
    await producer.start()

async def stop_producer():
    await producer.stop()
