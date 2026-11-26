import json
import os
import signal

import psycopg
from confluent_kafka import Consumer, KafkaException
from psycopg.types.json import Jsonb

TOPIC = "kilatmart.public.orders"
GROUP = "kilatmart-analytics"
INSERT_CHANGE = """
    INSERT INTO order_changes (
        topic, partition_id, kafka_offset, operation, order_id, order_ref, payload
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (topic, partition_id, kafka_offset) DO NOTHING
"""


def store_change(db, message):
    coordinates = (message.topic(), message.partition(), message.offset())
    if message.value() is None:
        return f"tombstone offset={coordinates[2]}"

    body = json.loads(message.value())
    key = json.loads(message.key())
    order_ref = (body["after"] or body["before"] or {}).get("order_ref")

    # Leaving the transaction commits the row before Kafka can advance.
    with db.transaction():
        db.execute(INSERT_CHANGE, (*coordinates, body["op"], key["id"], order_ref, Jsonb(body)))
    return f"offset={coordinates[2]} op={body['op']} order_id={key['id']} ref={order_ref}"


def process_message(db, consumer, message):
    if message.error():
        raise KafkaException(message.error())

    description = store_change(db, message)
    offsets = consumer.commit(message=message, asynchronous=False)
    for offset in offsets:
        if offset.error:
            raise KafkaException(offset.error)
    print(f"Committed {description}", flush=True)


def main():
    stopping = False

    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    consumer = Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })
    try:
        with psycopg.connect(os.environ["ANALYTICS_DSN"], autocommit=True) as db:
            consumer.subscribe([TOPIC])
            print(f"Subscribed to {TOPIC} with group {GROUP}", flush=True)
            while not stopping:
                message = consumer.poll(1.0)
                if message is not None:
                    process_message(db, consumer, message)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

