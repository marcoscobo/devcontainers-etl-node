import os, json, time, pandas as pd
from typing import List
from sqlalchemy import create_engine, text
from kafka import KafkaConsumer, TopicPartition

# -------- Config --------
PG_URL = os.getenv("PG_URL", "postgresql+psycopg2://postgres:postgres@postgres:5432/erp_db")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_MAX_RECORDS = int(os.getenv("KAFKA_MAX_RECORDS", "50000"))
KAFKA_POLL_TIMEOUT_MS = int(os.getenv("KAFKA_POLL_TIMEOUT_MS", "2000"))

# -------- Extractors --------
def read_pg_table(table: str) -> pd.DataFrame:
    """Read a table from Postgres into a DataFrame."""
    engine = create_engine(PG_URL, future=True)
    with engine.begin() as conn:
        conn.execute(text("SELECT 1"))
        return pd.read_sql(f"SELECT * FROM {table}", conn)

def read_kafka_topic(topic: str) -> pd.DataFrame:
    """Read all messages from the beginning of a Kafka topic into a DataFrame, then close."""
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )
    deadline = time.time() + 10
    partitions = None
    while time.time() < deadline and not partitions:
        partitions = consumer.partitions_for_topic(topic)
        consumer.poll(timeout_ms=200)
    if not partitions:
        raise SystemExit(f"❌ Kafka: Topic '{topic}' not found or has no partitions")
    tps = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(tps)
    for tp in tps:
        consumer.seek_to_beginning(tp)
    rows: List[dict] = []
    msgs_read = 0
    while msgs_read < KAFKA_MAX_RECORDS:
        batch = consumer.poll(timeout_ms=KAFKA_POLL_TIMEOUT_MS)
        if not batch:
            break
        for records in batch.values():
            for msg in records:
                rows.append(msg.value)
                msgs_read += 1
                if msgs_read >= KAFKA_MAX_RECORDS:
                    break
    consumer.close()
    return pd.DataFrame(rows)