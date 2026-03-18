"""
Database connection manager for KICKZ EMPIRE ELT pipeline.
Uses SQLAlchemy to connect to AWS RDS PostgreSQL.

TP1 — Step 0: Configure the database connection.
"""

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration from .env
# ---------------------------------------------------------------------------
RDS_HOST = os.getenv("RDS_HOST")
RDS_PORT = os.getenv("RDS_PORT", "5432")
RDS_DATABASE = os.getenv("RDS_DATABASE")
RDS_USER = os.getenv("RDS_USER")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")

BRONZE_SCHEMA = os.getenv("BRONZE_SCHEMA", "bronze_group0")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver_group0")
GOLD_SCHEMA = os.getenv("GOLD_SCHEMA", "gold_group0")


def get_engine():
    """
    Create and return a SQLAlchemy engine connected to PostgreSQL (AWS RDS).

    Returns:
        sqlalchemy.Engine: The connection engine.

    SQLAlchemy URL example:
        postgresql://user:password@host:port/database

    Docs:
        https://docs.sqlalchemy.org/en/20/core/engines.html
    """
    # TODO: Build the PostgreSQL connection URL and create the engine
    # Hint: use create_engine() from SQLAlchemy
    # The URL must follow this format: postgresql://{user}:{password}@{host}:{port}/{database}
    load_dotenv(dotenv_path=".env")
    password = quote_plus(RDS_PASSWORD)

    url = f"postgresql://{RDS_USER}:{password}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}"
    print("HOST =", RDS_HOST)

    return create_engine(url)


def test_connection():
    """
    Test the database connection.
    Executes a simple query (SELECT 1) and prints the result.

    Returns:
        bool: True if the connection succeeds, False otherwise.
    """
    # TODO: Use get_engine() to connect and execute SELECT 1
    # Hint: use engine.connect() inside a with block
    #       then connection.execute(text("SELECT 1"))
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        return result.scalar() == 1


def execute_sql(sql: str, params: dict = None):
    """
    Execute an arbitrary SQL query.

    Args:
        sql (str): The SQL query to execute.
        params (dict, optional): Query parameters.

    Returns:
        The query result (for SELECT), None for other statements.
    """
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        conn.commit()
        return result


# ---------------------------------------------------------------------------
# Entry point to test the connection
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("🔌 Testing connection to PostgreSQL (AWS RDS)...")
    if test_connection():
        print(f"✅ Connected successfully!")
        print(f"   Schemas: {BRONZE_SCHEMA}, {SILVER_SCHEMA}, {GOLD_SCHEMA}")
    else:
        print("❌ Connection failed. Check your .env file")
