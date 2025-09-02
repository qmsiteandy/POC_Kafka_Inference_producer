import json
import sys
import traceback

from kafka import KafkaProducer as Producer
from kafka.errors import KafkaError
import time
import random
import string
import requests


KAFKA_PRODUCER_VER = "2.3.0.1"
KAFKA_BROKERS = ["localhost:9092"]
KAFKA_TOPIC = "M2M_image"

KAFKA_IMAGE_MAX_SEG_BYTES = 300000  # 300 KB


class KafkaProducer:
    """
    Python producer
    """

    producer = None

    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(KafkaProducer, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """init producer"""
        # if hasattr(self, "_initialized") and self._initialized:
        #     return

        try:
            self.producer = Producer(
                bootstrap_servers=KAFKA_BROKERS,
                # security_protocol='SASL_SSL',
                # sasl_mechanism='SCRAM-SHA-256',
                # sasl_plain_username='user01',
                # sasl_plain_password='password',
                # ssl_cafile='CARoot.pem',
                # ssl_keyfile='key.pem',
                # ssl_certfile='certificate.pem',
                # api_version=(0, 11, 0),
            )
            self._initialized = True
            # logger.info("{} connected to broker: {}, topic: {}".format(self.__class__.__name__, KAFKA_BROKERS, KAFKA_TOPIC))
            print(
                f"{self.__class__.__name__} connected to broker: {KAFKA_BROKERS}, topic: {KAFKA_TOPIC}"
            )

        except ConnectionError as ex:
            raise Exception("{} connect fail: {}".format(ex, KAFKA_BROKERS))
        except Exception as ex:
            raise Exception("Exception: {}".format(repr(ex)))

    def produce(
        self, topic: str, headers: dict, payload: dict | str | bytes, key: str = None
    ):
        """
        produce a message to Kafka

        Notes
        - key: Optional key for the message, used for partitioning. Same key will always route to the same partition.
        """

        try:

            if self.producer is None:
                return 0

            # Kafka headers requirements: List of tuples of (str, bytes)
            # Convert headers to a list of header key value pairs. List items are tuples of str key and bytes value.
            headers = [(k, str(v).encode("utf-8")) for k, v in headers.items()]

            # Convert payload to bytes if it's not already
            payload = (
                json.dumps(payload)
                if not isinstance(payload, (str, bytes))
                else payload
            )
            payload = payload.encode("utf-8") if isinstance(payload, str) else payload

            # Convert key into bytes
            key = key.encode("utf-8") if isinstance(key, str) else key

            future = self.producer.send(
                topic=topic, key=key, headers=headers, value=payload
            )
            try:
                record_metadata = future.get(timeout=1)
                print("produce message bytes: {}".format(len(payload)))
                print(
                    "delivered, partition:{}, offset {}".format(
                        record_metadata.partition, record_metadata.offset
                    )
                )
                return 1
            except KafkaError as err:
                self.logger.error("KafkaError: {}".format(err))
        except Exception as ex:
            self.logger.error("Exception: {}".format(repr(ex)))
            self.logger.error(
                "Exception in produce_text() {}".format(sys.exc_info()[0])
            )
            self.logger.error(traceback.format_exc())
            return -1

    def produce_image_list(self, img_meta_list: list):
        """
        Input: [{'img_bytes': b'...', 'img_meta': {...}}]
        """
        # ----- Functions -----

        def _segment_image(image_bytes):
            """Segment the image into different parts"""

            file_size = len(image_bytes)
            segments = []
            for i in range(0, file_size, KAFKA_IMAGE_MAX_SEG_BYTES):
                index_from = i
                index_to = min(i + KAFKA_IMAGE_MAX_SEG_BYTES, file_size)
                segments.append(image_bytes[index_from:index_to])
                print(
                    f"Segmented bytes from {index_from} to {index_to-1}, size: {index_to - index_from}"
                )

            return segments

        # ----- Image Processing -----

        message_count = 0

        # Loop through images
        for img_meta in img_meta_list:

            # Get content and meta
            img_bytes = img_meta["img_bytes"]
            img_meta = img_meta["img_meta"]

            # Segment the image into smaller parts
            image_segments = _segment_image(img_bytes)

            # Record current byte offset
            current_offset = 0

            for idx, segment in enumerate(image_segments):

                # Format headers
                headers = {
                    "type": 3,
                    "seg": idx,
                    "total_seg": len(image_segments),
                    "total_sz": len(img_bytes),
                    "blk_offset": current_offset,
                    "file_meta_in_mongo": img_meta,
                }

                # Record offset
                current_offset += len(segment)

                result = self.produce(
                    topic=KAFKA_TOPIC,
                    headers=headers,
                    payload=segment,
                    key=img_meta["fileToken"],
                )

                if result == 1:
                    message_count += 1

        print(f"Send message to {KAFKA_TOPIC} count: {message_count}")

    def close(self):
        """Close the producer connection"""
        if self.producer is not None:
            self.producer.flush()  # Ensure all messages are sent
            self.producer.close()
            self.producer = None


class ImageProducer:

    def __init__(self):

        self.kafka_handler = KafkaProducer()

    def read_bytes_from_url(self, url):
        """Read bytes from a file URL"""
        response = requests.get(url)
        response.raise_for_status()
        return response.content

    def process(self, meta_list: list = []):

        image_meta_list = []

        for meta in meta_list:

            # Get file bytes
            url = meta.get("fileUrl")
            if not url:
                continue

            try:
                image_bytes = self.read_bytes_from_url(url)
            except Exception as e:
                print(f"Skip image due to error reading {url}: {e}")
                continue

            image_meta_list.append({"img_bytes": image_bytes, "img_meta": meta})

        self.kafka_handler.produce_image_list(image_meta_list)


if __name__ == "__main__":
    image_producer = ImageProducer()
    image_producer.process(
        meta_list=[
            {
                "_id": {"$oid": "683d1df4d63f51e55143f24b"},
                "fileToken": "KINSUS-HQ-UT03-Bot3_2025060201_1_624C0903-000 3208_0_7_7188_8584_4_2_17_003",
                "owner": {"Id": "0b231f8b6ac43e465e3e18b0b89d1258", "displayName": ""},
                "callerID": "KINSUS-HQ-UT03-Bot3;VISION",
                "deviceID": "SUBSTRATE_UTECHZONE",
                "dlVisionNames": ["VISION"],
                "dlVisions": [
                    {
                        "dlVisionName": "VISION",
                        "moduleName": "SUBSTRATE_UTECHZONE",
                        "version": "v0.2.0.0.20250507182704.793893",
                    }
                ],
                "ifmt": 4,
                "info": {
                    "productName": "bt01",
                    "productSKU": "",
                    "partID": "",
                    "subPartID": "",
                    "campus": "",
                    "plant": "",
                    "line": "",
                    "stationName": "",
                    "dataType": "",
                    "partNumber": "",
                },
                "isn": ["KINSUS-HQ-UT03-Bot3_2025060201_1_624C0903-000 3208"],
                "moduleName": [{"SUBSTRATE_UTECHZONE": 0}],
                "result": "P",
                "createdTime": {"$date": "2025-06-02T03:43:47.000Z"},
                "fileFolder": "recv/2025-06-02/11-00-00",
                "fileName": "0_7_7188_8584_4_2_17_003.bmp",
                "fileSize": 66652,
                "fileType": "image",
                "fileUrl": "https://techcrunch.com/wp-content/uploads/2023/01/GettyImages-1243527327-e1674211045958.jpg",
                "producerTime": {"$date": "2025-06-02T03:43:47.000Z"},
                "resolution": {"w": 600, "h": 300},
            }
        ]
    )
