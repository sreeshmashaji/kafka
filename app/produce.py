from fastapi import APIRouter
from kafka_producer import produce_message

router = APIRouter()

@router.post("/produce")
async def produce_message_endpoint(topic: str,key:str, message: str):
    await produce_message(topic, key,message)
    return {"Message": "Message sent"}
