# TP1 — ELT Pipeline: Bronze → Silver → Gold

## 🎯 Objective

Build a complete ELT pipeline that:
1. **Extracts** raw data (CSV from S3) → loads it into PostgreSQL (**Bronze** layer)
2. **Transforms** Bronze data → cleans and conforms it (**Silver** layer)
3. **Aggregates** Silver data → creates analytical tables (**Gold** layer)

```
S3 (CSV)  ──→  🥉 Bronze (raw)  ──→  🥈 Silver (clean)  ──→  🥇 Gold (analytics)
```

---

## 📋 Prerequisites

- Python 3.10+
- Access to the PostgreSQL database (AWS RDS) — credentials in `student_credentials.csv`
- AWS credentials (access key + secret key) to read from the S3 data lake — provided by the instructor

---

## Step 0 — Environment Setup (15 min)

### 0.1 Clone the repo and install dependencies

```bash
git clone <repo-url>
cd imt-elt-code

python -m venv venv
source venv/bin/activate   # macOS/Linux
# .\venv\Scripts\activate  # Windows

pip install -r requirements.txt
```

### 0.2 Configure the `.env`

```bash
cp .env.example .env
```

Edit `.env` with your credentials (provided by the instructor):

```env
# PostgreSQL (AWS RDS)
RDS_HOST=your-instance.eu-west-3.rds.amazonaws.com
RDS_PORT=5432
RDS_DATABASE=kickz_empire
RDS_USER=your_first_last        # e.g. alice_martin
RDS_PASSWORD=your_password      # from student_credentials.csv

# Schemas (replace 0 with your group number)
BRONZE_SCHEMA=bronze_group0
SILVER_SCHEMA=silver_group0
GOLD_SCHEMA=gold_group0

# AWS S3 data lake (read-only)
S3_BUCKET_NAME=kickz-empire-data
S3_PREFIX=raw
AWS_REGION=eu-west-3
AWS_ACCESS_KEY_ID=your_access_key     # from student_credentials.csv
AWS_SECRET_ACCESS_KEY=your_secret_key  # from student_credentials.csv
```

### 0.3 Implement the DB connection

📁 **File:** `src/database.py`

Complete the two functions marked `TODO`:

1. **`get_engine()`** — Create a SQLAlchemy engine
   - Build the URL: `postgresql://{user}:{password}@{host}:{port}/{database}`
   - Return `create_engine(url)`

2. **`test_connection()`** — Test the connection
   - Use `get_engine().connect()`
   - Execute `SELECT 1`
   - Return `True` on success, `False` otherwise

### 0.4 Verify

```bash
python -m src.database
```

Expected output:
```
🔌 Testing connection to PostgreSQL (AWS RDS)...
✅ Successfully connected!
   Schemas: bronze_group0, silver_group0, gold_group0
```

> ✅ **Checkpoint**: You must see the success message before continuing.

---

## Step 1 — Extract: S3 → Bronze (30 min)

📁 **File:** `src/extract.py`

### Principle

The Bronze layer stores data **as-is**, without any transformation. It is a faithful copy of the source. We keep even the "dirty" columns (`_internal_*`), as they will be cleaned in the next step.

The raw data lives in the **S3 data lake** (`s3://kickz-empire-data/raw/`). Your code will use **boto3** to read CSV files directly from S3.

### 1.1 Implement `_read_csv_from_s3()`

This function downloads a CSV from S3 and returns it as a DataFrame:

```python
def _read_csv_from_s3(s3_key):
    s3 = _get_s3_client()
    response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    csv_content = response["Body"].read().decode("utf-8")
    return pd.read_csv(StringIO(csv_content))
```

### 1.2 Implement `_load_to_bronze()`

This function loads a pandas DataFrame into a PostgreSQL table:

```python
def _load_to_bronze(df, table_name, if_exists="replace"):
    engine = get_engine()
    df.to_sql(
        name=table_name,
        con=engine,
        schema=BRONZE_SCHEMA,
        if_exists=if_exists,
        index=False,
    )
    print(f"    ✅ {BRONZE_SCHEMA}.{table_name} — {len(df)} rows loaded")
```

### 1.3 Implement the 4 `extract_*()` functions

Each function follows the same pattern:

```python
def extract_products():
    df = _read_csv_from_s3(f"{S3_PREFIX}/catalog/products.csv")
    print(f"  📦 Products: {len(df)} rows, {len(df.columns)} columns")
    _load_to_bronze(df, "products")
    return df
```

Do the same for:
- `extract_users()` → `"raw/users/users.csv"` → `"users"`
- `extract_orders()` → `"raw/orders/orders.csv"` → `"orders"`
- `extract_order_line_items()` → `"raw/order_line_items/order_line_items.csv"` → `"order_line_items"`

### 1.4 Implement `extract_all()`

Call the 4 functions and store the results:

```python
results["products"] = extract_products()
results["users"] = extract_users()
results["orders"] = extract_orders()
results["order_line_items"] = extract_order_line_items()
```

### 1.5 Verify

```bash
python -m src.extract
```

Expected output:
```
  🥉 EXTRACT → Bronze (bronze_group0)

  📦 Products: 229 rows, 21 columns
    ✅ bronze_group0.products — 229 rows loaded
  👤 Users: 5001 rows, 28 columns
    ✅ bronze_group0.users — 5001 rows loaded
  🛍️ Orders: 17073 rows, 30 columns
    ✅ bronze_group0.orders — 17073 rows loaded
  📋 Line items: 30885 rows, 16 columns
    ✅ bronze_group0.order_line_items — 30885 rows loaded

  ✅ Extraction complete — 4 tables loaded into bronze_group0
```

> ✅ **Checkpoint**: Check in PostgreSQL that the 4 tables exist in your bronze schema.

---

## Step 2 — Transform: Bronze → Silver (45 min)

📁 **File:** `src/transform.py`

### Principle

The Silver layer contains **cleaned and conformed** data:
- ❌ No more internal columns (`_*`)
- ❌ No more sensitive PII (passwords, IPs)
- ✅ Correct data types
- ✅ NULL values handled
- ✅ Validated data

### 2.1 Implement `_drop_internal_columns()`

```python
def _drop_internal_columns(df):
    internal_cols = [col for col in df.columns if col.startswith('_')]
    df = df.drop(columns=internal_cols)
    print(f"    🧹 {len(internal_cols)} internal columns removed: {internal_cols}")
    return df
```

### 2.2 Implement `transform_products()`

Follow the steps in the code comments:
1. Remove `_*` columns
2. Normalize `tags` (replace `|` with `,`)
3. Validate `price_usd > 0`
4. Convert booleans
5. Load into Silver

### 2.3 Implement `transform_users()`

⚠️ **PII Warning**: The Silver layer must NEVER contain passwords, IPs, or fingerprints.

1. Remove `_*` columns (8 columns)
2. Replace NULL `loyalty_tier` with `"none"`
3. Normalize emails (lowercase)
4. Load into Silver

### 2.4 Implement `transform_orders()`

1. Remove `_*` columns (6 columns)
2. Validate statuses
3. Convert `order_date` to datetime
4. Replace NULL `coupon_code` with `""`
5. Load into Silver

### 2.5 Implement `transform_order_line_items()`

1. Remove `_*` columns (3 columns)
2. Validate `quantity > 0`
3. Check `line_total` calculation
4. Load into Silver

### 2.6 Implement `transform_all()`

### 2.7 Verify

```bash
python -m src.transform
```

> ✅ **Checkpoint**: Verify that Silver tables have FEWER columns than Bronze tables (the `_*` columns are gone).

---

## Step 3 — Gold Layer: Analytics Aggregations (30 min)

📁 **File:** `src/gold.py`

### Principle

The Gold layer contains **business-ready** tables: aggregated, joined, and optimized for dashboards.

### 3.1 Implement `create_daily_revenue()`

Two possible approaches:

**Option A — SQL (recommended)**:
```python
def create_daily_revenue():
    query = f"""
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
    df = pd.read_sql(query, get_engine())
    _create_gold_table(df, "daily_revenue")
```

**Option B — Pandas**:
```python
def create_daily_revenue():
    orders = _read_silver("fct_orders")
    lines = _read_silver("fct_order_lines")
    # Filter cancellations
    orders = orders[~orders["status"].isin(["cancelled", "chargeback"])]
    # Merge and group
    merged = orders.merge(lines, on="order_id", how="left")
    daily = merged.groupby(merged["order_date"].dt.date).agg(...)
    _create_gold_table(daily, "daily_revenue")
```

### 3.2 Implement `create_product_performance()`

Join `fct_order_lines` ↔ `dim_products`, grouped by product.

### 3.3 Implement `create_customer_ltv()`

Join `fct_orders` ↔ `dim_users`, grouped by customer.

### 3.4 Verify

```bash
python -m src.gold
```

> ✅ **Checkpoint**: Verify that the Gold tables exist and contain coherent aggregated data.

---

## Step 4 — Run the Full Pipeline 🚀

```bash
python pipeline.py
```

Or step by step:
```bash
python pipeline.py --step extract
python pipeline.py --step transform
python pipeline.py --step gold
```

---

## 📊 Final Checks

Run these SQL queries to validate your pipeline:

```sql
-- Number of tables per schema
SELECT schemaname, COUNT(*) as nb_tables
FROM pg_tables
WHERE schemaname IN ('bronze_group0', 'silver_group0', 'gold_group0')
GROUP BY schemaname;

-- Bronze has more columns than Silver (_ columns removed)
SELECT COUNT(*) as nb_cols FROM information_schema.columns
WHERE table_schema = 'bronze_group0' AND table_name = 'products';
-- vs
SELECT COUNT(*) as nb_cols FROM information_schema.columns
WHERE table_schema = 'silver_group0' AND table_name = 'dim_products';

-- Top 5 products by revenue (Gold)
SELECT product_name, brand, total_revenue
FROM gold_group0.product_performance
ORDER BY total_revenue DESC
LIMIT 5;

-- Daily revenue
SELECT * FROM gold_group0.daily_revenue ORDER BY order_date;
```

---

## 🎁 Bonus (if you finish early)

1. **Add a 5th table**: Integrate `payment_transactions.csv` into the pipeline (Bronze → Silver → Gold)
2. **UPSERT method**: Modify `_load_to_bronze()` to use a MERGE/UPSERT instead of `replace` (idempotent)
3. **Logging**: Add a `print()` with timestamp at each step to trace execution
4. **Data schema**: Draw the ER diagram of your Silver layer (draw.io or Mermaid)

---

## 📚 Resources

- [SQLAlchemy 2.0 Documentation](https://docs.sqlalchemy.org/en/20/)
- [pandas.DataFrame.to_sql()](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html)
- [pandas.read_sql()](https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html)
- [Medallion Architecture (Bronze/Silver/Gold)](https://www.databricks.com/glossary/medallion-architecture)
