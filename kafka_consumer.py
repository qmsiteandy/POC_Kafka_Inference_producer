#!/usr/bin/env python3
"""
Kafka consumer to continuously consume and print m2m_list messages.
Run this after starting Kafka with: docker-compose up -d
"""

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import requests

KAFKA_BROKERS = ["172.18.212.212:9092"]
KAFKA_TOPIC = "OTA"

API_URL = "http://localhost:18089/api/v2/InferenceData"
SITE = "K1"


def message_post_inferencedata(message):
    try:

        # Hander from Kafka header list to dict.
        headers_dict = {
            k.decode("utf-8") if isinstance(k, bytes) else k: (
                v.decode("utf-8") if isinstance(v, bytes) else v
            )
            for k, v in message.headers
        }

        # Add site in header.
        headers_dict["site"] = SITE

        # Prepare m2m_log
        m2m_log = (
            message.value.decode("utf-8")
            if isinstance(message.value, bytes)
            else str(message.value)
        )

        payload = {
            "m2m_list": [
                {
                    "m2m_log": m2m_log,
                    "m2m_meta": headers_dict,
                }
            ]
        }

        headers = {"Content-Type": "application/json"}

        response = requests.post(API_URL, json=payload, headers=headers, timeout=10)
        response.raise_for_status()

        print(f"✅ Successfully sent to API - Status: {response.status_code}")
        return True

    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Error preparing payload: {e}")
        return False


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

            message_post_inferencedata(message)

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
