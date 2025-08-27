#!/usr/bin/env python3
"""
Kafka consumer to continuously consume and print m2m_list messages.
Run this after starting Kafka with: docker-compose up -d
"""

from kafka import KafkaConsumer
from kafka.errors import KafkaError

KAFKA_BROKERS = ["localhost:9092"]
KAFKA_TOPIC = "M2M"

def consume_m2m_messages():
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="test_consumer",
            # value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        print("🚀 Starting Kafka consumer for m2m_inference_data topic...")
        print("📝 Consuming messages (Press Ctrl+C to stop):")
        print("-" * 80)

        for message in consumer:
            print(f"📦 Received message:")
            print(f"   Topic: {message.topic}")
            print(f"   Partition: {message.partition}")
            print(f"   Offset: {message.offset}")
            print(f"   Timestamp: {message.timestamp}")

            print(f"   Header: {message.headers}")

            print(f"   Value: {message.value}")

            print("-" * 80)

    except KeyboardInterrupt:
        print("\n🛑 Consumer stopped by user")
    except KafkaError as e:
        print(f"❌ Kafka error: {e}")
        print("Make sure Kafka is running with: docker-compose up -d")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        try:
            consumer.close()
            print("✅ Consumer closed successfully")
        except:
            pass


if __name__ == "__main__":
    consume_m2m_messages()
