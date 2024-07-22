from aiokafka import AIOKafkaConsumer

from kafka_admin import create_topic_if_not_exist

async def consume_messages(topic: list):
    create_topic_if_not_exist(topic)

    
    consumer = AIOKafkaConsumer(
        *topic, bootstrap_servers="localhost:9092", group_id="my-group"
    )
    await consumer.start()
    try:
        print("inside consumer")
        async for msg in consumer:
            print(f"Received message from topic {msg.topic}, key {msg.key.decode() if msg.key else None}: {msg.value.decode()}")
    finally:
        await consumer.stop()
