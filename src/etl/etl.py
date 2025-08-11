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

def transform_for_clickhouse():
    """Transform extracted DataFrames for ClickHouse."""
    dfs = extract_dataframes()
    # Products and costumers dimensions
    dim_products = dfs["products"].copy()
    dim_products = dim_products.rename(columns={
        "id": "product_id",
        "name": "product_name"
    })
    dim_products["created_at"] = pd.to_datetime(dim_products["created_at"], utc=True)
    # Clientes dimension
    dim_customers = dfs["customers"].copy()
    dim_customers = dim_customers.rename(columns={
        "id": "customer_id"
    })
    dim_customers["created_at"] = pd.to_datetime(dim_customers["created_at"], utc=True)
    # Sales dimension
    fact_sales = dfs["sales_events"].copy()
    fact_sales["ts"] = pd.to_datetime(fact_sales["ts"], utc=True)
    fact_sales = fact_sales.merge(
        dim_customers[["customer_id", "external_id", "name", "segment"]],
        on="customer_id", how="left"
    ).merge(
        dim_products[["product_id", "sku", "product_name", "category"]],
        on="product_id", how="left"
    )
    # Inventory dimension
    fact_inventory = dfs["inventory"].copy()
    fact_inventory["date"] = pd.to_datetime(fact_inventory["date"], utc=True)
    return {
        "dim_products": dim_products,
        "dim_customers": dim_customers,
        "fact_sales": fact_sales,
        "fact_inventory": fact_inventory
    }

def main():
    tables = transform_for_clickhouse()
    for name, df in tables.items():
        print(f"\n=== {name} ({df.shape[0]} rows) ===")
        print(df.head(5).to_string(index=False))
        print("...")

if __name__ == "__main__":
    main()