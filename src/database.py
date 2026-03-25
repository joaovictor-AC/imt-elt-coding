"""
SOLUTION — Database connection manager.
"""

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

RDS_HOST = os.getenv("RDS_HOST")
RDS_PORT = os.getenv("RDS_PORT", "5432")
RDS_DATABASE = os.getenv("RDS_DATABASE")
RDS_USER = os.getenv("RDS_USER")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")

BRONZE_SCHEMA = os.getenv("BRONZE_SCHEMA", "bronze_group0")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver_group0")
GOLD_SCHEMA = os.getenv("GOLD_SCHEMA", "gold_group0")


def get_engine():
    """Create and return a SQLAlchemy engine connected to PostgreSQL."""
    # TODO: Build the PostgreSQL connection URL and create the engine
    # Hint: use create_engine() from SQLAlchemy
    # The URL must follow this format: postgresql://{user}:{password}@{host}:{port}/{database}
    url = f"postgresql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}"
    return create_engine(url)


def test_connection():
    """Test the connection to the database."""
    # TODO: Use get_engine() to connect and execute SELECT 1
    # Hint: use engine.connect() inside a with block
    #       then connection.execute(text("SELECT 1"))
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            row = result.fetchone()
            print(f"  SELECT 1 = {row[0]}")
        return True
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False


def execute_sql(sql: str, params: dict = None):
    """Execute an arbitrary SQL query."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        conn.commit()
        return result


if __name__ == "__main__":
    print("🔌 Testing connection to PostgreSQL (AWS RDS)...")
    if test_connection():
        print(f"✅ Successfully connected!")
        print(f"   Schemas: {BRONZE_SCHEMA}, {SILVER_SCHEMA}, {GOLD_SCHEMA}")
    else:
        print("❌ Connection failed. Check your .env")
