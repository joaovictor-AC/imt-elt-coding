"""
KICKZ EMPIRE — Extract (Bronze Layer)
======================================
TP1 — Step 2: Load raw data from S3 → Bronze schema in PostgreSQL.

This module reads files from the S3 data lake (bucket: kickz-empire-data)
and loads them as-is into your group's Bronze schema.

The data lake contains **3 different file formats**:
    - CSV   : simple tabular files
    - JSONL : one JSON object per line (newline-delimited JSON)
    - Parquet (partitioned) : columnar format, split by date (dt=YYYY-MM-DD/)

Datasets handled in this lab:
    CSV:
        1. raw/catalog/products.csv                         → bronze.products
        2. raw/users/users.csv                              → bronze.users
        3. raw/orders/orders.csv                            → bronze.orders
        4. raw/order_line_items/order_line_items.csv        → bronze.order_line_items
    JSONL:
        5. raw/reviews/reviews.jsonl                        → bronze.reviews
    Parquet (partitioned by day):
        6. raw/clickstream/dt=YYYY-MM-DD/part-*.snappy.parquet → bronze.clickstream

Bronze principle: data is loaded AS-IS, with zero transformations.
We even keep the "dirty" columns (_internal_*, _hashed_password, etc.)
"""

import os
import json
from io import StringIO, BytesIO

import boto3
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import text

from src.database import get_engine, BRONZE_SCHEMA
from src.logger import get_logger

logger = get_logger(__name__) 
# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "kickz-empire-data")
S3_PREFIX = os.getenv("S3_PREFIX", "raw")   # root prefix in the bucket
AWS_REGION = os.getenv("AWS_REGION", "eu-west-3")


# ---------------------------------------------------------------------------
# Helpers — S3 client
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


# ---------------------------------------------------------------------------
# Helpers — Read different formats from S3
# ---------------------------------------------------------------------------
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
    # Steps: get S3 client → get_object() → read & decode the body → pd.read_csv()
    # Remember: read_csv() expects a file-like object, not a raw string
    
    s3 = _get_s3_client()
    response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))

    return df    

def _read_jsonl_from_s3(s3_key: str) -> pd.DataFrame:
    """
    Read a JSONL (newline-delimited JSON) file from S3 into a DataFrame.

    JSONL format = one JSON object per line. Each line is a complete record.
    Example line: {"review_id": "abc", "rating": 5, "body": "Great!"}

    Args:
        s3_key (str): Full S3 object key (e.g. "raw/reviews/reviews.jsonl")

    Returns:
        pd.DataFrame: The JSONL contents.

    Hint: use boto3 to get the object, then pd.read_json() with lines=True.
    Docs:
        https://pandas.pydata.org/docs/reference/api/pandas.read_json.html
    """
    # TODO: Download the JSONL from S3 and return it as a DataFrame
    # Very similar to _read_csv_from_s3(), but use pd.read_json() instead.
    # Key parameter: lines=True (tells pandas each line is a separate JSON object)
    
    s3 = _get_s3_client()
    response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    df = pd.read_json(StringIO(response['Body'].read().decode('utf-8')), lines=True)
    
    return df
    
def _read_partitioned_parquet_from_s3(s3_prefix: str) -> pd.DataFrame:
    """
    Read a date-partitioned Parquet dataset from S3 into a DataFrame.

    The clickstream data is stored as partitioned Parquet:
        raw/clickstream/dt=2026-02-05/part-00001.snappy.parquet
        raw/clickstream/dt=2026-02-05/part-00002.snappy.parquet
        raw/clickstream/dt=2026-02-06/part-00001.snappy.parquet
        ...

    Strategy:
        1. List all objects under the given S3 prefix
        2. Filter for .parquet files only
        3. Download each file and read it with pyarrow
        4. Concatenate all partitions into a single DataFrame

    Args:
        s3_prefix (str): S3 prefix for the partitioned dataset
                         (e.g. "raw/clickstream/")

    Returns:
        pd.DataFrame: All partitions concatenated.

    Docs:
        https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html
    """
    # TODO: List all Parquet files under s3_prefix and concatenate them
    # Strategy:
    #   1. Use s3.get_paginator("list_objects_v2") to list all objects under the prefix
    #   2. Filter keys that end with ".parquet"
    #   3. For each file: download with get_object(), read with pq.read_table()
    #      (Parquet is binary → use BytesIO, not StringIO)
    #   4. Collect all DataFrames in a list, then pd.concat() them
    
    s3 = _get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=s3_prefix)
    dfs = []
    
    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.parquet'):
                response = s3.get_object(Bucket=S3_BUCKET, Key=key)
                table = pq.read_table(BytesIO(response['Body'].read()))
                df = table.to_pandas()
                dfs.append(df)

    return pd.concat(dfs, ignore_index=True)

# ---------------------------------------------------------------------------
# Helper — Load to Bronze
# ---------------------------------------------------------------------------
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
    # You'll need: get_engine(), and the right to_sql() parameters
    # Don't forget: index=False (we don't want the pandas index as a column)
    
    engine = get_engine()
    df.to_sql(
        name=table_name,
        con=engine,
        schema=BRONZE_SCHEMA,
        if_exists=if_exists,
        index=False # exclude the pandas index from the SQL table

    )
    
# ---------------------------------------------------------------------------
# Extract functions — CSV datasets
# ---------------------------------------------------------------------------
def extract_products() -> pd.DataFrame:
    """
    Extract the product catalog from S3 and load it into bronze.products.

    Format: CSV
    S3 key: raw/catalog/products.csv

    Steps:
        1. Read products.csv from S3 using _read_csv_from_s3()
        2. Print the number of rows and columns
        3. Load into bronze.products using _load_to_bronze()

    Returns:
        pd.DataFrame: The catalog data.
    """
    # TODO: Read → Log → Load → Return
    # Use _read_csv_from_s3() with the right S3 key, then _load_to_bronze()

    try:
        logger.info("Extract: products")
        s3_key = f"{S3_PREFIX}/catalog/products.csv"
        df_products = _read_csv_from_s3(s3_key)
        
        logger.info(f"Products Rows: {df_products.shape[0]}, Columns: {df_products.shape[1]}")
        
        _load_to_bronze(df_products, table_name="products")
        
        return df_products
    except Exception as e:
        logger.error(f"Error extracting products: {str(e)}")
        raise


def extract_users() -> pd.DataFrame:
    """
    Extract users from S3 and load them into bronze.users.

    Format: CSV
    S3 key: raw/users/users.csv

    Returns:
        pd.DataFrame: The user data.
    """
    try:
        logger.info("Extract: users")
        s3_key = f"{S3_PREFIX}/users/users.csv"
        df = _read_csv_from_s3(s3_key)
        logger.info(f"users: {len(df)} rows, {len(df.columns)} columns")
        _load_to_bronze(df, "users")
        return df
    except Exception as e:
        logger.error(f"Error extracting users: {str(e)}")
        raise


def extract_orders() -> pd.DataFrame:
    """
    Extract orders from S3 and load them into bronze.orders.

    Format: CSV
    S3 key: raw/orders/orders.csv

    Returns:
        pd.DataFrame: The order data.
    """
    try:
        logger.info("Extract: orders")
        s3_key = f"{S3_PREFIX}/orders/orders.csv"
        df_orders = _read_csv_from_s3(s3_key)
        
        logger.info(f"Orders Rows: {df_orders.shape[0]}, Columns: {df_orders.shape[1]}")
        
        _load_to_bronze(df_orders, table_name="orders")
        
        return df_orders
    except Exception as e:
        logger.error(f"Error extracting orders: {str(e)}")
        raise


def extract_order_line_items() -> pd.DataFrame:
    """
    Extract order line items from S3 and load them into bronze.order_line_items.

    Format: CSV
    S3 key: raw/order_line_items/order_line_items.csv

    Returns:
        pd.DataFrame: The order line item data.
    """
    try:
        logger.info("Extract: order_line_items")
        s3_key = f"{S3_PREFIX}/order_line_items/order_line_items.csv"
        df_order_line_items = _read_csv_from_s3(s3_key)
        
        logger.info(f"Order Line Items Rows: {df_order_line_items.shape[0]}, Columns: {df_order_line_items.shape[1]}")
        
        _load_to_bronze(df_order_line_items, table_name="order_line_items")
        
        return df_order_line_items
    except Exception as e:
        logger.error(f"Error extracting order_line_items: {str(e)}")
        raise


# ---------------------------------------------------------------------------
# Extract functions — JSONL datasets
# ---------------------------------------------------------------------------
def extract_reviews() -> pd.DataFrame:
    """
    Extract customer reviews from S3 and load them into bronze.reviews.

    Format: JSONL (newline-delimited JSON)
    S3 key: raw/reviews/reviews.jsonl

    Steps:
        1. Read reviews.jsonl from S3 using _read_jsonl_from_s3()
        2. Print the number of rows and columns
        3. Load into bronze.reviews using _load_to_bronze()

    Returns:
        pd.DataFrame: The reviews data.
    """
    try:
        logger.info("Extract: reviews")
        s3_key = f"{S3_PREFIX}/reviews/reviews.jsonl"
        df_reviews = _read_jsonl_from_s3(s3_key)
        
        logger.info(f"Reviews Rows: {df_reviews.shape[0]}, Columns: {df_reviews.shape[1]}")
        
        _load_to_bronze(df_reviews, table_name="reviews")
        
        return df_reviews
    except Exception as e:
        logger.error(f"Error extracting reviews: {str(e)}")
        raise


# ---------------------------------------------------------------------------
# Extract functions — Parquet datasets (partitioned)
# ---------------------------------------------------------------------------
def extract_clickstream() -> pd.DataFrame:
    """
    Extract clickstream events from S3 and load them into bronze.clickstream.

    Format: Partitioned Parquet (Snappy compressed)
    S3 prefix: raw/clickstream/
    Structure:
        raw/clickstream/dt=2026-02-05/part-00001.snappy.parquet
        raw/clickstream/dt=2026-02-05/part-00002.snappy.parquet
        raw/clickstream/dt=2026-02-06/part-00001.snappy.parquet
        ...

    This is a large dataset (~544k rows, 30 days, 28 columns).
    The data is partitioned by date (dt=YYYY-MM-DD) and split into
    multiple part files per day.

    Steps:
        1. Read all partitions using _read_partitioned_parquet_from_s3()
        2. Print the number of rows and columns
        3. Load into bronze.clickstream using _load_to_bronze()

    Returns:
        pd.DataFrame: The clickstream data.
    """
    try:
        logger.info("Extract: clickstream")
        s3_prefix = f"{S3_PREFIX}/clickstream/"
        df_clickstream = _read_partitioned_parquet_from_s3(s3_prefix)
        
        logger.info(f"Clickstream Rows: {df_clickstream.shape[0]}, Columns: {df_clickstream.shape[1]}")
        
        _load_to_bronze(df_clickstream, table_name="clickstream")
        
        return df_clickstream
    except Exception as e:
        logger.error(f"Error extracting clickstream: {str(e)}")
        raise


# ---------------------------------------------------------------------------
# 🎁 Bonus
# ---------------------------------------------------------------------------

def extract_payments() -> pd.DataFrame:
    """
    Bonus: Extract payments from S3 and load them into bronze.payments.

    Format: CSV
    S3 key: raw/payments/payment_transactions.csv

    Returns:
        pd.DataFrame: The payment data.
    """

    try:
        logger.info("Extract: payments")
        s3_key = f"{S3_PREFIX}/payments/payment_transactions.csv"
        df_payments = _read_csv_from_s3(s3_key)
        
        logger.info(f"Payments Rows: {df_payments.shape[0]}, Columns: {df_payments.shape[1]}")
        
        _load_to_bronze(df_payments, table_name="payments")
        
        return df_payments
    except Exception as e:
        logger.error(f"Error extracting payments: {str(e)}")
        raise

def extract_inventory() -> pd.DataFrame:
    """
    Bonus: Extract inventory from S3 and load them into bronze.inventory.

    Format: CSV
    S3 key: raw/inventory/inventory_movements.csv

    Returns:
        pd.DataFrame: The inventory data.
    """
    try:
        logger.info("Extract: inventory")
        s3_key = f"{S3_PREFIX}/inventory/inventory_movements.csv"
        df_inventory = _read_csv_from_s3(s3_key)
        
        logger.info(f"Inventory Rows: {df_inventory.shape[0]}, Columns: {df_inventory.shape[1]}")
        
        _load_to_bronze(df_inventory, table_name="inventory")
        
        return df_inventory
    except Exception as e:
        logger.error(f"Error extracting inventory: {str(e)}")
        raise

def extract_marketing() -> pd.DataFrame:
    """
    Bonus: Extract marketing data from S3 and load them into bronze.marketing.

    Format: JSONL
    S3 key: raw/marketing/marketing_events.jsonl

    Returns:
        pd.DataFrame: The marketing data.
    """

    try:
        logger.info("Extract: marketing")
        s3_key = f"{S3_PREFIX}/marketing/marketing_events.jsonl"
        df_marketing = _read_jsonl_from_s3(s3_key)
        
        logger.info(f"Marketing Rows: {df_marketing.shape[0]}, Columns: {df_marketing.shape[1]}")
        
        _load_to_bronze(df_marketing, table_name="marketing")
        
        return df_marketing
    except Exception as e:
        logger.error(f"Error extracting marketing: {str(e)}")
        raise

def extract_searc_events() -> pd.DataFrame:
    """
    Bonus: Extract search events from S3 and load them into bronze.search_events.

    Format: JSONL
    S3 prefix: raw/search_events/search_events.jsonl

    Returns:
        pd.DataFrame: The search events data.
    """

    try:
        logger.info("Extract: search_events")
        s3_prefix = f"{S3_PREFIX}/search_events/search_events.jsonl"
        df_search_events = _read_jsonl_from_s3(s3_prefix)
        
        logger.info(f"Search Events Rows: {df_search_events.shape[0]}, Columns: {df_search_events.shape[1]}")
        
        _load_to_bronze(df_search_events, table_name="search_events")
        
        return df_search_events
    except Exception as e:
        logger.error(f"Error extracting search_events: {str(e)}")
        raise

def extract_abandoned_carts() -> pd.DataFrame:
    """
    Bonus: Extract abandoned cart events from S3 and load them into bronze.abandoned_carts.

    Format: JSONL
    S3 key: raw/abandoned_carts/abandoned_carts.jsonl

    Returns:
        pd.DataFrame: The abandoned carts data.
    """

    try:
        logger.info("Extract: abandoned_carts")
        s3_key = f"{S3_PREFIX}/abandoned_carts/abandoned_carts.jsonl"
        df_abandoned_carts = _read_jsonl_from_s3(s3_key)
        
        logger.info(f"Abandoned Carts Rows: {df_abandoned_carts.shape[0]}, Columns: {df_abandoned_carts.shape[1]}")
        
        df_abandoned_carts['items'] = df_abandoned_carts['items'].apply(json.dumps)
        
        _load_to_bronze(df_abandoned_carts, table_name="abandoned_carts")
        
        return df_abandoned_carts
    except Exception as e:
        logger.error(f"Error extracting abandoned_carts: {str(e)}")
        raise

def extract_interactions() -> pd.DataFrame:
    """
    Bonus: Extract customer interactions from S3 and load them into bronze.interactions.

    Format: Partitioned Parquet (Snappy compressed)
    S3 key: raw/interactions/

    Returns:
        pd.DataFrame: The customer interactions data.
    """

    try:
        logger.info("Extract: interactions")
        s3_key = f"{S3_PREFIX}/interactions/"
        df_interactions = _read_partitioned_parquet_from_s3(s3_key)
        
        logger.info(f"Interactions Rows: {df_interactions.shape[0]}, Columns: {df_interactions.shape[1]}")
        
        _load_to_bronze(df_interactions, table_name="interactions")
        
        return df_interactions
    except Exception as e:
        logger.error(f"Error extracting interactions: {str(e)}")
        raise

# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------
def extract_all() -> dict[str, pd.DataFrame]:
    """
    Run the full extraction of all sources into Bronze.

    Extracts 6 datasets across 3 formats:
        - 4 CSV files  (products, users, orders, order_line_items)
        - 1 JSONL file  (reviews)
        - 1 Partitioned Parquet dataset (clickstream)

    Returns:
        dict: A dictionary {table_name: DataFrame} for each extracted table.
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"  🥉 EXTRACT → Bronze ({BRONZE_SCHEMA})")
    logger.info(f"{'='*60}\n")

    results = {}

    # TODO: Call each extract_*() function and store the result in the dict
    # There are 6 functions to call: 4 CSV + 1 JSONL + 1 Parquet

    results['products'] = extract_products()
    results['users'] = extract_users()
    results['orders'] = extract_orders()
    results['order_line_items'] = extract_order_line_items()
    results['reviews'] = extract_reviews()
    results['clickstream'] = extract_clickstream()
    results['payments'] = extract_payments()
    results['inventory'] = extract_inventory()
    results['marketing'] = extract_marketing()
    results['search_events'] = extract_searc_events()
    results['abandoned_carts'] = extract_abandoned_carts()
    results['interactions'] = extract_interactions()

    logger.info(f"\n  ✅ Extraction complete — {len(results)} tables loaded into {BRONZE_SCHEMA}")
    return results


# ---------------------------------------------------------------------------
# Entry point to test extraction alone
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    extract_all()
