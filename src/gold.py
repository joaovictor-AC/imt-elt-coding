"""
KICKZ EMPIRE — Gold Layer (Business Aggregations)
==================================================
TP1 — Step 3: Create Gold tables/views from Silver.

The Gold layer contains aggregated tables and views, ready for
analytics dashboards and business reports.

Gold tables/views created:
    1. gold.daily_revenue       — Revenue per day
    2. gold.product_performance — Product performance (sales, revenue, qty)
    3. gold.customer_ltv        — Lifetime Value per customer
"""

import pandas as pd
from sqlalchemy import text

from src.database import get_engine, SILVER_SCHEMA, GOLD_SCHEMA


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _read_silver(table_name: str) -> pd.DataFrame:
    """Read a table from the Silver schema."""
    engine = get_engine()
    query = f"SELECT * FROM {SILVER_SCHEMA}.{table_name}"
    return pd.read_sql(query, engine)


def _create_gold_table(df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
    """Load a DataFrame into a Gold schema table."""
    engine = get_engine()
    df.to_sql(
        name=table_name,
        con=engine,
        schema=GOLD_SCHEMA,
        if_exists=if_exists,
        index=False,
    )
    print(f"    ✅ {GOLD_SCHEMA}.{table_name} — {len(df)} rows")


def _create_gold_view(view_name: str, sql: str):
    """
    Create a SQL view in the Gold schema.

    Args:
        view_name (str): View name (without the schema).
        sql (str): The SELECT query that defines the view.
    """
    engine = get_engine()
    full_name = f"{GOLD_SCHEMA}.{view_name}"
    with engine.connect() as conn:
        conn.execute(text(f"DROP VIEW IF EXISTS {full_name}"))
        conn.execute(text(f"CREATE VIEW {full_name} AS {sql}"))
        conn.commit()
    print(f"    ✅ View {full_name} created")


# ---------------------------------------------------------------------------
# Gold tables / views
# ---------------------------------------------------------------------------
def create_daily_revenue():
    """
    Create gold.daily_revenue — Revenue per day.

    Expected columns:
        - order_date (DATE)      : Day
        - total_orders (INT)     : Number of orders
        - total_revenue (FLOAT)  : Sum of total_usd
        - avg_order_value (FLOAT): Average order value
        - total_items (INT)      : Total number of items sold

    Source: silver.fct_orders (join with silver.fct_order_lines)

    SQL Hint:
        SELECT
            DATE(o.order_date) AS order_date,
            COUNT(DISTINCT o.order_id) AS total_orders,
            SUM(o.total_usd) AS total_revenue,
            AVG(o.total_usd) AS avg_order_value,
            SUM(ol.quantity) AS total_items
        FROM {SILVER_SCHEMA}.fct_orders o
        LEFT JOIN {SILVER_SCHEMA}.fct_order_lines ol ON o.order_id = ol.order_id
        WHERE o.status NOT IN ('cancelled', 'chargeback')
        GROUP BY DATE(o.order_date)
        ORDER BY order_date
    """
    print("  📊 Gold: daily_revenue")

    # TODO: Option A — SQL method (recommended for performance)
    # Write the SQL query above and use pd.read_sql() + _create_gold_table()

    # TODO: Option B — Pandas method (alternative)
    # orders = _read_silver("fct_orders")
    # lines = _read_silver("fct_order_lines")
    # ... aggregations with groupby ...

    raise NotImplementedError("TODO: Implement create_daily_revenue()")


def create_product_performance():
    """
    Create gold.product_performance — Metrics per product.

    Expected columns:
        - product_id (STRING)     : Product ID
        - product_name (STRING)   : Product name
        - brand (STRING)          : Brand
        - category (STRING)       : Category
        - total_quantity_sold (INT) : Total quantity sold
        - total_revenue (FLOAT)   : Total revenue
        - num_orders (INT)        : Number of orders
        - avg_unit_price (FLOAT)  : Average selling price

    Source: silver.fct_order_lines JOIN silver.dim_products

    Hint:
        Group by product_id, product_name, brand, category
        Aggregate quantity, line_total_usd, count distinct order_id
    """
    print("  🏆 Gold: product_performance")

    # TODO: Create the product_performance table
    # Join fct_order_lines + dim_products to get name and category
    # Group by product and aggregate sales

    raise NotImplementedError("TODO: Implement create_product_performance()")


def create_customer_ltv():
    """
    Create gold.customer_ltv — Lifetime Value per customer.

    Expected columns:
        - user_id (INT)           : Customer ID
        - email (STRING)          : Email
        - first_name (STRING)     : First name
        - last_name (STRING)      : Last name
        - loyalty_tier (STRING)   : Loyalty tier
        - total_orders (INT)      : Total number of orders
        - total_spent (FLOAT)     : Total amount spent
        - avg_order_value (FLOAT) : Average order value
        - first_order_date (DATE) : First order date
        - last_order_date (DATE)  : Last order date
        - days_as_customer (INT)  : Customer tenure in days

    Source: silver.fct_orders JOIN silver.dim_users

    Hint:
        Group by user_id, join with dim_users for customer info
        - first_order_date = MIN(order_date)
        - last_order_date = MAX(order_date)
        - days_as_customer = last_order_date - first_order_date
    """
    print("  💰 Gold: customer_ltv")

    # TODO: Create the customer_ltv table
    # Join fct_orders + dim_users
    # Group by customer and aggregate

    raise NotImplementedError("TODO: Implement create_customer_ltv()")


# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------
def create_gold_layer():
    """
    Create all Gold layer tables/views.
    """
    print(f"\n{'='*60}")
    print(f"  🥇 GOLD Layer ({GOLD_SCHEMA})")
    print(f"{'='*60}\n")

    # TODO: Call each Gold creation function
    # create_daily_revenue()
    # create_product_performance()
    # create_customer_ltv()

    raise NotImplementedError("TODO: Implement create_gold_layer()")

    print(f"\n  ✅ Gold layer created in {GOLD_SCHEMA}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    create_gold_layer()
