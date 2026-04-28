import json
import logging
import os
from datetime import datetime, timedelta

from confluent_kafka import Consumer, KafkaException

logger = logging.getLogger(__name__)

TOPICS = ["system_1", "system_2", "system_3"]
POLL_TIMEOUT_SECONDS = 5.0
MAX_EMPTY_POLLS = 3
INCREMENTAL_GROUP_ID = "identity_pipeline"


def make_consumer(date, backfill=False):
    group_id = f"identity_pipeline_{date}" if backfill else INCREMENTAL_GROUP_ID
    consumer = Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(TOPICS)
    return consumer


def consume_events(consumer, date, backfill=False):
    day_end = datetime.fromisoformat(date)
    day_start = day_end - timedelta(days=1)

    events = []
    empty_polls = 0

    while empty_polls < MAX_EMPTY_POLLS:
        msg = consumer.poll(timeout=POLL_TIMEOUT_SECONDS)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            raise KafkaException(msg.error())

        empty_polls = 0
        raw = msg.value().decode("utf-8")
        payload = json.loads(raw)

        if backfill:
            event_ts = datetime.fromisoformat(payload["ts"])
            if not (day_start <= event_ts < day_end):
                continue

        events.append({
            "email": payload["email"],
            "ts": payload["ts"],
            "ip": payload["ip"],
            "event_json": raw,
            "source_topic": msg.topic(),
        })

    return events