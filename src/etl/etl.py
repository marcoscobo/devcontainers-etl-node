# etl.py
import pandas as pd
from typing import Dict
from extractors import read_pg_table, read_kafka_topic, read_minio_inventory_df

# -------- Helpers --------
def extract_dataframes() -> Dict[str, pd.DataFrame]:
    """Return dict of DataFrames: products, customers, sales_events, inventory."""
    return {
        "products": read_pg_table("products"),
        "customers": read_pg_table("customers"),
        "sales_events": read_kafka_topic(),
        "inventory": read_minio_inventory_df(),
    }

def main():
    dfs = extract_dataframes()
    for name, df in dfs.items():
        print(f"[{name}] shape={df.shape}")
        if not df.empty:
            print(df.head(5).to_string(index=False))
            print(df.tail(5).to_string(index=False))

if __name__ == "__main__":
    main()