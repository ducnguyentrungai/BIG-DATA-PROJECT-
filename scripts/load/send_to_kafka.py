# import json
# from kafka import KafkaProducer

# def load_to_kafka(data, topic: str, kafka_server: str = 'kafka:9092'):
#     print(f"📤 Sending to Kafka topic '{topic}'...")
#     producer = KafkaProducer(
#         bootstrap_servers=kafka_server,
#         value_serializer=lambda x: json.dumps(x).encode('utf-8'),
#         request_timeout_ms=30000
#     )
#     for _, row in data.iterrows():
#         record = json.loads(json.dumps(row.to_dict(), default=str))
#         producer.send(topic, value=record)

#     producer.flush()
#     producer.close()
#     print(f"✅ All data sent to Kafka topic '{topic}'")

import json
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

# def load_to_kafka(data: pd.DataFrame, topic: str, kafka_server: str = 'kafka:9092'):
#     """
#     Gửi dữ liệu từ pandas DataFrame lên Kafka topic, từng dòng là 1 message JSON.

#     Args:
#         data (pd.DataFrame): Dữ liệu cần gửi (mỗi dòng 1 record).
#         topic (str): Tên Kafka topic đích.
#         kafka_server (str): Địa chỉ Kafka broker (mặc định: kafka:9092).
#     """
#     print(f"📤 Bắt đầu gửi dữ liệu đến Kafka topic: '{topic}'")

#     # Kiểm tra dữ liệu đầu vào
#     if data is None or data.empty:
#         print("⚠️ DataFrame rỗng, không có gì để gửi.")
#         return

#     try:
#         # Khởi tạo Kafka Producer
#         producer = KafkaProducer(
#             bootstrap_servers=kafka_server,
#             value_serializer=lambda x: json.dumps(x).encode('utf-8'),
#             retries=5,  # Tự động thử lại nếu gửi lỗi tạm thời
#             linger_ms=10,  # Gộp nhiều message nhỏ
#             request_timeout_ms=30000
#         )

#         # Gửi từng dòng dữ liệu lên Kafka
#         for i, (_, row) in enumerate(data.iterrows()):
#             try:
#                 # Chuyển dòng thành JSON dict
#                 record = json.loads(json.dumps(row.to_dict(), default=str))

#                 # Gửi lên Kafka
#                 future = producer.send(topic, value=record)

#                 # Tuỳ chọn: xử lý kết quả phản hồi (blocking)
#                 metadata = future.get(timeout=10)

#                 print(f"✅ Record {i+1} sent: Partition={metadata.partition}, Offset={metadata.offset}")

#             except KafkaError as ke:
#                 print(f"❌ Lỗi gửi dòng {i+1}: {ke}")
#             except Exception as e:
#                 print(f"❌ Lỗi khác tại dòng {i+1}: {e}")

#         # Đảm bảo tất cả message đã được gửi
#         producer.flush()
#         print("✅ Đã gửi toàn bộ dữ liệu tới Kafka.")

#     except Exception as e:
#         print(f"❌ Lỗi kết nối Kafka hoặc khởi tạo Producer: {e}")

#     finally:
#         try:
#             producer.close()
#         except Exception:
#             pass
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
