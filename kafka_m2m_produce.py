import json
import sys
import traceback

from kafka import KafkaProducer as Producer
from kafka.errors import KafkaError
import time
import random
import string


KAFKA_PRODUCER_VER = "2.3.0.1"
KAFKA_BROKERS = ["localhost:9092"]
KAFKA_TOPIC = "M2M_data"


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

    def produce(self, headers: dict, payload: dict):
        """kafka produce"""

        try:

            if self.producer is None:
                return 0

            # Convert headers to a list of header key value pairs. List items are tuples of str key and bytes value.
            headers = [(k, str(v).encode("utf-8")) for k, v in headers.items()]

            # Convert payload to bytes if it's not already
            payload = (
                json.dumps(payload)
                if not isinstance(payload, (str, bytes))
                else payload
            )
            payload = payload.encode("utf-8") if isinstance(payload, str) else payload

            future = self.producer.send(
                topic=KAFKA_TOPIC, headers=headers, value=payload
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
                print("KafkaError: {}".format(err))
        except Exception as ex:
            print("Exception: {}".format(repr(ex)))
            print("Exception in produce_text() {}".format(sys.exc_info()[0]))
            print(traceback.format_exc())
            return -1

    def produce_m2m_list(self, data_list: list):
        """Produce a list of messages to Kafka"""
        if not data_list:
            return 0

        count = 0

        for data_dict in data_list:

            result = self.produce(
                headers=data_dict["m2m_meta"], payload=data_dict["m2m_log"]
            )
            if result == 1:
                count += 1

    def close(self):
        """Close the producer connection"""
        if self.producer is not None:
            self.producer.flush()  # Ensure all messages are sent
            self.producer.close()
            self.producer = None


if __name__ == "__main__":

    def generate_file_token():
        return "".join(random.choices(string.ascii_lowercase + string.digits, k=32))

    current_time_ms = int(time.time() * 1000)
    file_token = generate_file_token()

    m2m_list = [
        {
            "m2m_log": json.dumps(
                {
                    "DeviceID": "inspect_flow",
                    "DeviceDesc": None,
                    "LogDate": {"$date": current_time_ms},
                    "Message": {
                        "LogType": "VISION",
                        "ErrorCode": "",
                        "StateDesc": "COMPLETION",
                        "WarnLevel": "TRACE",
                        "errorCode": "",
                        "VisionMsg": {
                            "PDNM": "product0827_1",
                            "LOT": "lot0827_1_1",
                            "COD": "cod1",
                            "STRIP": "strip1",
                            "GRAIN": "grain1",
                            "IFID": "",
                            "CLEEID": [],
                            "MOD": "model1",
                            "IFMT": 4,
                            "IMGNM": "19_21_6598_11147_7_5_00",
                            "CT": 0,
                            "PART": "model1",
                            "PARTN": "part0",
                            "PRDT": 0,
                            "CLERID": "HQ-9F_1",
                            "SWVER": "v0.1",
                            "SESS": "KINSUS-HQ-UT03-TOP1_17071063134821_001_RH621A020200-0001",
                            "JR": {
                                "FR": "P",
                                "ORIR": "P",
                                "UH": "sampled",
                            },
                            "IFR": {"inference": [{}, {}]},
                            "GT": {
                                "annotation": [
                                    {"category_id": 2, "category_name": "NA"}
                                ]
                            },
                            "GTR": {"FR": [{"result": "F", "from": "test1"}]},
                        },
                        "LogMessage": "<1-None><4-None>",
                    },
                }
            ),
            "m2m_meta": {
                "version": "2.3.0.1",
                "type": 0,
                "device_id": "",
                "IP": "",
                "MAC": "",
                "seg": 1,
                "total_seg": 1,
                "total_sz": 862,
                "blk_offset": 0,
                "blk_sz": 862,
                "name": f"{file_token}.jpg",
                "ts1": str(time.time()),
                "ts2": str(time.time()),
                "prj_token": "656d7f9c67576fcd2ced2252",
                "token": file_token,
                "custom_info": '{"MOD": "model1", "LogType": "VISION"}',
            },
        }
    ]

    # Send messages intervally (every 2 seconds, 5 times as an example)
    kafka_producer = KafkaProducer()
    for _ in range(100):
        kafka_producer.produce_m2m_list(m2m_list)
        time.sleep(2)
    kafka_producer.close()
