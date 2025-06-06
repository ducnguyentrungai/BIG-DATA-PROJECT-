import json
from kafka import KafkaProducer

def load_to_kafka(data, topic: str, kafka_server: str = 'kafka:9092'):
    print(f"📤 Sending to Kafka topic '{topic}'...")
    producer = KafkaProducer(
        bootstrap_servers=kafka_server,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        request_timeout_ms=30000
    )

    for _, row in data.iterrows():
        record = json.loads(json.dumps(row.to_dict(), default=str))
        producer.send(topic, value=record)

    producer.flush()
    producer.close()
    print(f"✅ All data sent to Kafka topic '{topic}'")
