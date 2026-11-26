# KilatMart A1: Change data capture

KilatMart's web shop writes orders to PostgreSQL. Debezium reads changes from
the PostgreSQL write-ahead log, Kafka retains the change events, and a Python
consumer writes an event history to a separate analytics database.

## Start

Use Docker Desktop or Docker Engine with Compose v2. Give Docker at least 4 GB
of memory. The first run downloads the images and builds the consumer.

```console
docker compose up
```

Compose registers the connector automatically. The five seed orders use three
products and are captured by the initial snapshot. Wait until the consumer logs
five `op=r` records. The project publishes no ports to the host.

## Live demo

In another terminal, change to this folder and run the script with Python 3.10
or later. It uses the standard library and the Docker CLI.

```console
python demo.py
```

1. Insert one paid order and show its `c` event in analytics. The timer starts
   before the insert command and ends when the row is first observed. It
   includes Compose and query polling overhead.
2. Stop only the consumer with `docker compose stop consumer`. Insert two
   more orders. Check that both writes succeed and Kafka lag rises to 2
   while zero new rows appear in analytics.
3. Start the same consumer with `docker compose start consumer`. Check that
   both rows appear at the queued offsets, then check lag returns to 0.

Run on a dedicated instance of the project without concurrent demo runs or
other inserts. Each run creates its own order references. A successful run
writes `demo-evidence.json` with the observed references, consumer offsets,
lag and timings. Compare the measured live timing with the assignment's
“within seconds” expectation.

Useful inspection commands:

```console
docker compose ps -a
docker compose logs --tail=15 connect register-connector consumer
docker compose exec -T kafka /kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group kilatmart-analytics
docker compose exec -T analytics-db psql -U demo -d analytics -c "SELECT order_ref, operation, kafka_offset FROM order_changes ORDER BY kafka_offset;"
```

## What each component does

| Component | Responsibility |
|---|---|
| `orders-db` | Owns `shop.public.orders` and the `kilatmart_orders` publication. |
| `connect` | Runs the PostgreSQL connector with `pgoutput` and publishes to `kilatmart.public.orders`. |
| `kafka` | Holds the source topic, Connect offsets and the consumer group's committed offsets. |
| `consumer` | Stores the envelope in `analytics.public.order_changes`, then commits the Kafka offset. |
| `analytics-db` | Holds the analytics change history. |

The consumer subscribes as `kilatmart-analytics`. Its committed offset is the
next record to read. If the database commit succeeds and the offset commit
fails, the same Kafka record can be redelivered. The row's unique Kafka
coordinates let PostgreSQL ignore that repeated delivery. Separately produced
events at different Kafka offsets can still describe the same source change.
Debezium's replication position is separate from the analytics group's offset.

The source table is tracked for inserts, updates and deletes. The consumer
keeps the original Debezium envelopes. Table deletes arrive as delete events,
with the primary key available even if other old values are absent. Kafka's
null-valued tombstones advance the consumer offset without adding an analytics
row. The sink is an event history, so an update or delete adds a history entry.

This is log-based, asynchronous data integration. It removes analytics
polling and timing dependencies between order writes and analytics processing.
It covers the extract and load jobs of classical ETL. Business transforms are
outside this demo. Catch-up depends on retained Kafka records. This single
broker demonstrates consumer downtime without providing broker failover.
The database credentials are for the isolated local demo.

## Stop

`docker compose down` stops services and preserves the named volumes. The
snapshot happens on first setup. For a clean rehearsal, set a new
`COMPOSE_PROJECT_NAME` in both terminals before starting the stack and demo.

## Present and submit

Allow about four minutes for slides and three minutes for the live demo.
Start Compose before presenting. Run `python demo.py` in rehearsal, retain the
evidence, and transfer the observed timings and lag to slide 5. Run the same
command while screen recording for the backup video. Keep that video under
two minutes. Add your group's repository URL to slide 1 before exporting the
submission PDF.

## References

- [Debezium tutorial](https://debezium.io/documentation/reference/3.4/tutorial.html)
- [Debezium PostgreSQL connector](https://debezium.io/documentation/reference/3.4/connectors/postgresql.html)
- [Kafka Python client and synchronous commits](https://docs.confluent.io/kafka-clients/python/current/index.html)
