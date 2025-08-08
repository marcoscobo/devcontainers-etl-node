import os, csv, random, pathlib, tempfile, time, boto3
from datetime import datetime, date, timedelta
from botocore.config import Config
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, Dict, List

# ---------- Config ----------
PG_URL = os.getenv("PG_URL", "postgresql+psycopg2://postgres:postgres@postgres:5432/erp_db")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_REGION = os.getenv("S3_REGION", "us-east-1")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "wms-snapshots")
NUM_WAREHOUSES   = int(os.getenv("NUM_WAREHOUSES", "3"))
MAGNITUDE_ORDER  = int(os.getenv("MAGNITUDE_ORDER", "50"))
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "30"))
STEP_DAYS        = int(os.getenv("STEP_DAYS", "7"))
START_DATE_STR   = os.getenv("START_DATE", "2025-01-01")
USE_PREFIX_TREE = os.getenv("USE_PREFIX_TREE", "true").strip().lower() in ("1", "true", "yes", "y", "on")

# ---------- Helpers ----------
def s3_client():
    """Create and return a boto3 S3 client configured for MinIO/S3-compatible storage."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )

def ensure_bucket(s3):
    """Ensure the target bucket exists; create it if it does not."""
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=S3_BUCKET)
            print(f"🪣 Created bucket: {S3_BUCKET}")
        else:
            raise e

def upload_file(s3, local_path: str, object_name: str):
    """Upload a local file to S3/MinIO at the given object key."""
    s3.upload_file(
        local_path,
        S3_BUCKET,
        object_name,
        ExtraArgs={"ContentType": "text/csv"},
    )
    print(f"📤 Uploaded → s3://{S3_BUCKET}/{object_name}")

def get_product_ids_from_pg(limit: Optional[int] = None) -> List[int]:
    """
    Fetch product IDs from Postgres to keep WMS snapshots consistent with ERP. If Postgres is not reachable or empty, fallback to synthetic IDs [1..50].
    """
    try:
        engine = create_engine(PG_URL, future=True)
        with engine.connect() as conn:
            sql = "SELECT id FROM products ORDER BY id"
            if limit:
                sql += f" LIMIT {int(limit)}"
            rows = conn.execute(text(sql)).all()
        ids = [int(r.id) for r in rows]
        if ids:
            print(f"✅ Postgres: loaded {len(ids)} product_id(s).")
            return ids
    except SQLAlchemyError as e:
        print(f"⚠️ Could not read products from Postgres: {e}")
    n = 50
    print(f"ℹ️ Falling back to synthetic product IDs (1..{n}).")
    return list(range(1, n + 1))

def write_inventory_csv(file_path: str, d: date, warehouse: str, product_ids: List[int]):
    """
    Write a CSV snapshot for a single warehouse and date (schema: date, product_id, warehouse_id, stock_units).
    """
    date_str = d.strftime("%Y-%m-%d")
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "product_id", "warehouse_id", "stock_units"])
        for pid in product_ids:
            stock = random.randint(1, MAGNITUDE_ORDER)
            w.writerow([date_str, pid, warehouse, stock])

# ---------- Main ----------
def main():
    # Parse starting date of the simulated timeline
    start_date = datetime.strptime(START_DATE_STR, "%Y-%m-%d").date()
    # Initialize an independent date pointer per warehouse
    warehouses = [f"wh-{i+1:02d}" for i in range(NUM_WAREHOUSES)]
    next_date_per_wh: Dict[str, date] = {wh: start_date for wh in warehouses}
    # S3 client and ensure bucket availability
    s3 = s3_client()
    ensure_bucket(s3)
    # Load product IDs from ERP (Postgres) for consistency in warehouse snapshots
    product_ids = get_product_ids_from_pg()
    print(
        f"🏁 Starting periodic generation: every {INTERVAL_SECONDS}s, "
        f"one file per warehouse, step {STEP_DAYS} day(s). Start date: {start_date}."
    )
    # Insert data periodically
    try:
        cycle = 0
        while True:
            cycle += 1
            uploaded = 0
            for wh in warehouses:
                d = next_date_per_wh[wh]
                ymd = d.strftime("%Y%m%d")
                filename = f"stock_{wh}_{ymd}.csv"
                if USE_PREFIX_TREE:
                    prefix = f"inventory/{d.strftime('%Y')}/{d.strftime('%m')}/"
                    object_name = f"{prefix}{filename}"
                else:
                    object_name = filename
                with tempfile.TemporaryDirectory() as tmpd:
                    local_path = str(pathlib.Path(tmpd) / filename)
                    write_inventory_csv(local_path, d, wh, product_ids)
                    upload_file(s3, local_path, object_name)
                # Advance this warehouse's simulated date pointer
                next_date_per_wh[wh] = d + timedelta(days=STEP_DAYS)
                uploaded += 1
            print(f"→ Cycle {cycle}: uploaded {uploaded} file(s) ({len(warehouses)} warehouses).")
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user. Exiting…")

if __name__ == "__main__":
    main()