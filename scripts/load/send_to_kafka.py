import json
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

def load_to_kafka(data: pd.DataFrame, topic: str, kafka_server: str = 'kafka:9092'):
    print(f"📤 Bắt đầu gửi dữ liệu đến Kafka topic: '{topic}'")

    if data is None or data.empty:
        print("⚠️ DataFrame rỗng, không có gì để gửi.")
        return

    producer = None
    success_count = 0
    fail_count = 0

    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            retries=5,
            linger_ms=10,
            request_timeout_ms=30000
        )

        for i, (_, row) in enumerate(data.iterrows()):
            try:
                record = json.loads(json.dumps(row.to_dict(), default=str))
                future = producer.send(topic, value=record)
                metadata = future.get(timeout=10)
                print(f"✅ Record {i+1} sent: Partition={metadata.partition}, Offset={metadata.offset}")
                success_count += 1
            except KafkaError as ke:
                print(f"❌ Kafka lỗi dòng {i+1}: {ke}")
                fail_count += 1
            except Exception as e:
                print(f"❌ Lỗi khác dòng {i+1}: {e}")
                fail_count += 1

        producer.flush()
        print(f"📦 Gửi xong {success_count} record, thất bại {fail_count}")

    except Exception as e:
        print(f"❌ Lỗi Kafka Producer: {e}")

    finally:
        if producer is not None:
            try:
                producer.close()
            except Exception:
                pass
