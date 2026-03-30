from fastapi import FastAPI, WebSocket
import json
from kafka import KafkaConsumer
app = FastAPI()
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    consumer = KafkaConsumer("patient-vitals", bootstrap_servers="localhost:9092",
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    for msg in consumer:
        await ws.send_json(msg.value)
