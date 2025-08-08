import os, pandas as pd
from typing import Dict
from extractors import read_pg_table, read_kafka_topic

# -------- Config --------
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sales.events")

# -------- Helpers --------
def extract_dataframes() -> Dict[str, pd.DataFrame]:
    """Return dict of DataFrames: products, customers, sales_events."""
    return {
        "products": read_pg_table("products"),
        "customers": read_pg_table("customers"),
        "sales_events": read_kafka_topic(KAFKA_TOPIC),
    }

def main():
    dfs = extract_dataframes()
    for name, df in dfs.items():
        print(f"[{name}] shape={df.shape}")
        if not df.empty:
            print(df.head(5).to_string(index=False))

if __name__ == "__main__":
    main()