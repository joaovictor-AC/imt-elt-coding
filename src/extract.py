"""
KICKZ EMPIRE — Extract (Bronze Layer)
======================================
TP1 — Step 1: Load raw data (CSV) from S3 → Bronze schema in PostgreSQL.

This module reads CSV files from the S3 data lake (bucket: kickz-empire-data)
and loads them as-is into your group's Bronze schema.

Datasets handled in this lab:
    1. raw/catalog/products.csv            → bronze.products
    2. raw/users/users.csv                 → bronze.users
    3. raw/orders/orders.csv               → bronze.orders
    4. raw/order_line_items/order_line_items.csv → bronze.order_line_items

Bronze principle: data is loaded AS-IS, with zero transformations.
We even keep the "dirty" columns (_internal_*, _hashed_password, etc.)
"""

import os
from io import StringIO

import boto3
import pandas as pd
from sqlalchemy import text

from src.database import get_engine, BRONZE_SCHEMA


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "kickz-empire-data")
S3_PREFIX = os.getenv("S3_PREFIX", "raw")   # root prefix in the bucket
AWS_REGION = os.getenv("AWS_REGION", "eu-west-3")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _get_s3_client():
    """
    Create and return a boto3 S3 client.

    Uses AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY from environment
    variables (loaded via .env).

    Returns:
        boto3.client: An S3 client configured for the project region.
    """
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def _read_csv_from_s3(s3_key: str) -> pd.DataFrame:
    """
    Read a CSV file from S3 into a pandas DataFrame.

    Args:
        s3_key (str): Full S3 object key (e.g. "raw/catalog/products.csv")

    Returns:
        pd.DataFrame: The CSV contents.

    Hint: use boto3 to get the object, then pd.read_csv() on the body.
    Docs:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
    """
    # TODO: Download the CSV from S3 and return it as a DataFrame
    # Hint:
    #   s3 = _get_s3_client()
    #   response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    #   csv_content = response["Body"].read().decode("utf-8")
    #   return pd.read_csv(StringIO(csv_content))
    raise NotImplementedError("TODO: Implement _read_csv_from_s3()")


def _load_to_bronze(df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
    """
    Load a DataFrame into a Bronze schema table.

    Args:
        df (pd.DataFrame): The data to load.
        table_name (str): Target table name (without the schema).
        if_exists (str): Behavior if the table already exists.
            - "replace": drop and recreate the table
            - "append" : add data to the existing table

    Hint: use df.to_sql() with these parameters:
        - name: table name
        - con: SQLAlchemy engine
        - schema: the bronze schema
        - if_exists: "replace" or "append"
        - index: False (don't write the pandas index)

    Docs:
        https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
    """
    # TODO: Load the DataFrame into PostgreSQL using df.to_sql()
    raise NotImplementedError("TODO: Implement _load_to_bronze()")


# ---------------------------------------------------------------------------
# Extract functions (one per dataset)
# ---------------------------------------------------------------------------
def extract_products() -> pd.DataFrame:
    """
    Extract the product catalog from S3 and load it into bronze.products.

    S3 key: raw/catalog/products.csv

    Steps:
        1. Read products.csv from S3 using _read_csv_from_s3()
        2. Print the number of rows and columns
        3. Load into bronze.products using _load_to_bronze()

    Returns:
        pd.DataFrame: The catalog data.
    """
    # TODO: Implement product catalog extraction
    # Hint:
    #   df = _read_csv_from_s3(f"{S3_PREFIX}/catalog/products.csv")
    #   print(f"  📦 Products: {len(df)} rows, {len(df.columns)} columns")
    #   _load_to_bronze(df, "products")
    #   return df
    raise NotImplementedError("TODO: Implement extract_products()")


def extract_users() -> pd.DataFrame:
    """
    Extract users from S3 and load them into bronze.users.

    S3 key: raw/users/users.csv

    Returns:
        pd.DataFrame: The user data.
    """
    # TODO: Implement user extraction
    # Same pattern as extract_products() but with "raw/users/users.csv" → "users"
    raise NotImplementedError("TODO: Implement extract_users()")


def extract_orders() -> pd.DataFrame:
    """
    Extract orders from S3 and load them into bronze.orders.

    S3 key: raw/orders/orders.csv

    Returns:
        pd.DataFrame: The order data.
    """
    # TODO: Implement order extraction
    # Same pattern with "raw/orders/orders.csv" → "orders"
    raise NotImplementedError("TODO: Implement extract_orders()")


def extract_order_line_items() -> pd.DataFrame:
    """
    Extract order line items from S3 and load them into bronze.order_line_items.

    S3 key: raw/order_line_items/order_line_items.csv

    Returns:
        pd.DataFrame: The order line item data.
    """
    # TODO: Implement order line item extraction
    # Same pattern with "raw/order_line_items/order_line_items.csv" → "order_line_items"
    raise NotImplementedError("TODO: Implement extract_order_line_items()")


# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------
def extract_all() -> dict[str, pd.DataFrame]:
    """
    Run the full extraction of all CSV sources into Bronze.

    Returns:
        dict: A dictionary {table_name: DataFrame} for each extracted table.
    """
    print(f"\n{'='*60}")
    print(f"  🥉 EXTRACT → Bronze ({BRONZE_SCHEMA})")
    print(f"{'='*60}\n")

    results = {}

    # TODO: Call each extract_*() function and store the result
    # Hint:
    #   results["products"] = extract_products()
    #   results["users"] = extract_users()
    #   results["orders"] = extract_orders()
    #   results["order_line_items"] = extract_order_line_items()

    raise NotImplementedError("TODO: Implement extract_all()")

    print(f"\n  ✅ Extraction complete — {len(results)} tables loaded into {BRONZE_SCHEMA}")
    return results


# ---------------------------------------------------------------------------
# Entry point to test extraction alone
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    extract_all()
