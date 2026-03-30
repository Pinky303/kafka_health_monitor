import boto3, json, time
from kafka import KafkaConsumer
consumer = KafkaConsumer("patient-vitals", bootstrap_servers="localhost:9092",
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
s3 = boto3.client("s3")
batch = []
for msg in consumer:
    batch.append(msg.value)
    if len(batch) >= 50:
        key = f"batch_{int(time.time())}.json"
        s3.put_object(Bucket="icu-vitals-archive", Key=key, Body=json.dumps(batch))
        batch = []
