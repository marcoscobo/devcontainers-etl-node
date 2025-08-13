import os, clickhouse_connect, pandas as pd
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional

# ---------- Config ----------
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_USER = os.getenv("CH_USER", "clickhouse")
CH_PASSWORD = os.getenv("CH_PASSWORD", "clickhouse")
CH_DATABASE = os.getenv("CH_DATABASE", "commerce")
CH_INTERFACE = os.getenv("CH_INTERFACE", "native").lower()
CH_NATIVE_PORT = int(os.getenv("CH_NATIVE_PORT", "9000"))
CH_HTTP_PORT = int(os.getenv("CH_HTTP_PORT", "8123"))
CH_CHUNK_ROWS = int(os.getenv("CH_CHUNK_ROWS", "200000"))
CH_ASYNC_INSERT = os.getenv("CH_ASYNC_INSERT", "1") in ("1", "true", "True", "YES", "yes")
CH_WAIT_ASYNC = os.getenv("CH_WAIT_ASYNC", "1") in ("1", "true", "True", "YES", "yes")

# ---------- Helpers ----------
def _get_client(database: str = "default"):
    """Create a ClickHouse client based on the configured interface."""
    def _make(interface: str, port: int):
        return clickhouse_connect.get_client(
            host=CH_HOST,
            port=port,
            username=CH_USER,
            password=CH_PASSWORD,
            database=database,
            interface=interface,
        )
    try:
        if CH_INTERFACE == "native":
            return _make("native", CH_NATIVE_PORT)
        else:
            return _make("http", CH_HTTP_PORT)
    except Exception:
        if CH_INTERFACE == "native":
            # print("⚠️ HTTP Fallback at 8123 port")
            return _make("http", CH_HTTP_PORT)
        raise

def _ensure_database():
    """Ensure the ClickHouse database exists."""
    admin = _get_client(database="default")
    admin.command(f"CREATE DATABASE IF NOT EXISTS {CH_DATABASE}")

def _create_tables():
    """Create ClickHouse tables if they do not exist."""
    ddl = {
        "dim_products": f"""
            CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dim_products (
                product_id UInt32,
                sku String,
                product_name String,
                category LowCardinality(String),
                price Decimal(12,2),
                currency LowCardinality(String),
                created_at DateTime64(6, 'UTC')
            )
            ENGINE = MergeTree
            ORDER BY product_id
        """,
        "dim_customers": f"""
            CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dim_customers (
                customer_id UInt32,
                external_id String,
                name String,
                email String,
                segment LowCardinality(String),
                created_at DateTime64(6, 'UTC')
            )
            ENGINE = MergeTree
            ORDER BY customer_id
        """,
        "fact_sales": f"""
            CREATE TABLE IF NOT EXISTS {CH_DATABASE}.fact_sales (
                event_id UUID,
                ts DateTime64(6, 'UTC'),
                customer_id UInt32,
                product_id UInt32,
                qty Int32,
                unit_price Decimal(12,2),
                external_id String,
                customer_name String,
                segment LowCardinality(String),
                sku String,
                product_name String,
                category LowCardinality(String)
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(ts)
            ORDER BY (ts, event_id)
        """,
        "fact_inventory": f"""
            CREATE TABLE IF NOT EXISTS {CH_DATABASE}.fact_inventory (
                date Date,
                product_id UInt32,
                warehouse_id LowCardinality(String),
                stock_units Int32,
                object String
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(date)
            ORDER BY (date, product_id, warehouse_id)
        """,
    }
    client = _get_client(database="default")
    for sql in ddl.values():
        client.command(sql)

def _to_decimal_2(x) -> Optional[Decimal]:
    """Convert a value to Decimal with 2 decimal places, or None if invalid."""
    if x is None or (isinstance(x, float) and pd.isna(x)) or (pd.isna(x) if hasattr(pd, "isna") else False):
        return None
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def _normalize_df(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Normalize DataFrame to match ClickHouse table schema."""
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)
    w = df.copy()
    # Rename columns if needed
    alias_map = {
        "customer_name": "name",
    }
    for target, source in alias_map.items():
        if target in cols and target not in w.columns and source in w.columns:
            w[target] = w[source]
    # Convert date/time columns to UTC and remove timezone info
    if "created_at" in w.columns:
        w["created_at"] = pd.to_datetime(w["created_at"], utc=True).dt.tz_convert(None)
    if "ts" in w.columns:
        w["ts"] = pd.to_datetime(w["ts"], utc=True).dt.tz_convert(None)
    if "date" in w.columns:
        w["date"] = pd.to_datetime(w["date"], utc=True).dt.date
    # Numeric columns to appropriate types
    for c in ["product_id", "customer_id", "qty", "stock_units"]:
        if c in w.columns:
            w[c] = pd.to_numeric(w[c], errors="coerce").fillna(0).astype("int64")
    for c in ["price", "unit_price"]:
        if c in w.columns:
            w[c] = w[c].apply(_to_decimal_2)
    # UUID conversion
    if "event_id" in w.columns:
        w["event_id"] = w["event_id"].astype(str)
    # Ensure all required columns are present
    missing = [c for c in cols if c not in w.columns]
    if missing:
        raise ValueError(f"⚠️ Required columns are missing: {missing}")
    w = w[cols].where(pd.notna(w), None)
    return w

def _insert_df(table: str, df: pd.DataFrame, cols: List[str], client: "clickhouse_connect.driver.Client"):
    """Insert DataFrame into ClickHouse table with normalization."""
    df2 = _normalize_df(df, cols)
    if df2.empty:
        print(f"🔘 [{table}] empty DataFrame; not inserted.")
        return
    total = len(df2)
    settings = {}
    if CH_ASYNC_INSERT:
        settings.update({"async_insert": 1})
        if CH_WAIT_ASYNC:
            settings.update({"wait_for_async_insert": 1})
    if CH_CHUNK_ROWS and total > CH_CHUNK_ROWS:
        inserted = 0
        for start in range(0, total, CH_CHUNK_ROWS):
            end = min(start + CH_CHUNK_ROWS, total)
            chunk = df2.iloc[start:end]
            client.insert_df(
                table=table,
                df=chunk,
                column_names=cols,
                database=CH_DATABASE,
                settings=settings or None,
            )
            inserted += len(chunk)
            print(f"➕ [{table}] Insertadas {inserted}/{total} filas...")
        print(f"➕ [{table}] Insertadas {total} filas (chunked).")
    else:
        client.insert_df(
            table=table,
            df=df2,
            column_names=cols,
            database=CH_DATABASE,
            settings=settings or None,
        )
        print(f"➕ [{table}] Insertadas {total} filas.")

def load_clickhouse_tables(tables: Dict[str, pd.DataFrame]) -> None:
    """Create ClickHouse tables and insert data from DataFrames."""
    _ensure_database()
    _create_tables()
    client = _get_client(database=CH_DATABASE)
    if "dim_products" in tables:
        _insert_df(
            "dim_products",
            tables["dim_products"],
            ["product_id", "sku", "product_name", "category", "price", "currency", "created_at"],
            client,
        )
    if "dim_customers" in tables:
        _insert_df(
            "dim_customers",
            tables["dim_customers"],
            ["customer_id", "external_id", "name", "email", "segment", "created_at"],
            client,
        )
    if "fact_sales" in tables:
        _insert_df(
            "fact_sales",
            tables["fact_sales"],
            [
                "event_id", "ts", "customer_id", "product_id", "qty", "unit_price",
                "external_id", "customer_name", "segment", "sku", "product_name", "category",
            ],
            client,
        )
    if "fact_inventory" in tables:
        _insert_df(
            "fact_inventory",
            tables["fact_inventory"],
            ["date", "product_id", "warehouse_id", "stock_units", "object"],
            client,
        )
    try:
        client.close()
    except Exception:
        pass
