# extractors.py
import os, json, time, re
import pandas as pd
from io import BytesIO
from typing import List, Dict, Optional
from sqlalchemy import create_engine, text
from kafka import KafkaConsumer, TopicPartition
import boto3
from botocore.config import Config

# -------- Config --------
PG_URL = os.getenv("PG_URL", "postgresql+psycopg2://postgres:postgres@postgres:5432/erp_db")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sales.events")
KAFKA_MAX_RECORDS = int(os.getenv("KAFKA_MAX_RECORDS", "50000"))
KAFKA_POLL_TIMEOUT_MS = int(os.getenv("KAFKA_POLL_TIMEOUT_MS", "2000"))
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_REGION = os.getenv("S3_REGION", "us-east-1")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "wms-snapshots")
S3_PREFIX = os.getenv("S3_PREFIX", "inventory/")

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name=S3_REGION,
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)

# -------- Helpers --------
def read_pg_table(table: str) -> pd.DataFrame:
    """Read a table from Postgres into a DataFrame."""
    engine = create_engine(PG_URL, future=True)
    with engine.begin() as conn:
        conn.execute(text("SELECT 1"))
        return pd.read_sql(f"SELECT * FROM {table}", conn)

def read_kafka_topic(topic: str = KAFKA_TOPIC) -> pd.DataFrame:
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

def _infer_date_from_key(key: str) -> Optional[pd.Timestamp]:
    """Try to infer a date from the S3 object key (supports YYYY-MM-DD or YYYYMMDD)."""
    for pat in [re.compile(r"(\d{4})[-_](\d{2})[-_](\d{2})")]:
        m = pat.search(key.replace("/", "-"))
        if m:
            y, mth, d = m.groups()
            try:
                return pd.to_datetime(f"{y}-{mth}-{d}", utc=False)
            except Exception:
                pass
    return None

def _list_s3_csv_keys(bucket: str, prefix: str) -> List[str]:
    """List all CSV object keys under a prefix (handles pagination)."""
    keys: List[str] = []
    kwargs: Dict[str, str] = {"Bucket": bucket, "Prefix": prefix}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for it in resp.get("Contents", []):
            key = it["Key"]
            if key.lower().endswith(".csv"):
                keys.append(key)
        if resp.get("IsTruncated"):
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        else:
            break
    return keys

def read_minio_inventory_df() -> pd.DataFrame:
    """Read all inventory CSVs from S3/MinIO (under S3_PREFIX) and return a single DataFrame. Ensure date column."""
    keys = _list_s3_csv_keys(S3_BUCKET, S3_PREFIX)
    frames: List[pd.DataFrame] = []
    for k in keys:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=k)
        df = pd.read_csv(BytesIO(obj["Body"].read()))
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], utc=False, errors="coerce")
        else:
            inferred = _infer_date_from_key(k)
            df["date"] = inferred
        df["object"] = k
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["date", "product_id", "warehouse_id", "stock_units", "__object"])
    out = pd.concat(frames, ignore_index=True)
    if "date" in out.columns:
        out = out.sort_values("date", kind="stable").reset_index(drop=True)
    return out