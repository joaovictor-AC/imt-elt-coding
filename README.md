# KICKZ EMPIRE — ELT Data Pipeline

## 1. Project Description

This project implements a complete ELT (Extract, Load, Transform) data pipeline for an e-commerce context.

The pipeline ingests raw operational data (products, users, orders, payments, etc.) from AWS S3, stores it in a PostgreSQL database (AWS RDS), cleans and standardizes it, and finally produces business-ready aggregated tables for analytics and reporting.

The business objective is to enable:
- reliable tracking of sales and revenue  
- customer behavior analysis  
- product performance monitoring  
- KPI generation for decision-making  

The project follows modern data engineering practices with a layered architecture and modular Python codebase.

---

## 2. Architecture Diagram

The pipeline follows a layered ELT architecture:

    Raw Data Sources (AWS S3)
                │
                ▼
        ┌────────────────┐
        │   Bronze Layer │
        │  (Extraction)  │
        └────────────────┘
                │
                ▼
        ┌────────────────┐
        │   Silver Layer │
        │ (Transformation)│
        └────────────────┘
                │
                ▼
        ┌────────────────┐
        │    Gold Layer  │
        │  (Aggregation) │
        └────────────────┘

- Bronze: raw data stored as-is in the database  
- Silver: cleaned and standardized data  
- Gold: aggregated and analytics-ready tables  

### Project Structure

```
imt-elt-coding-master/
│
├── pipeline.py              # Main pipeline orchestrator
├── requirements.txt         # Python dependencies
├── README.md                # Project documentation
│
├── src/                     # Core pipeline logic
│   ├── __init__.py
│   ├── database.py          # PostgreSQL connection & queries
│   ├── extract.py           # Data extraction (Bronze layer)
│   ├── transform.py         # Data cleaning & transformation (Silver)
│   ├── gold.py              # Aggregations & business tables (Gold)
│   ├── monitoring.py        # Pipeline monitoring & reporting
│   ├── logger.py            # Logging utilities
│   └── check_files.ipynb    # Data exploration notebook
│
├── tests/                   # Unit tests (pytest)
│   ├── __init__.py
│   ├── conftest.py          # Fixtures and mocks
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_gold.py
│
├── docs/                    # Additional documentation
│   ├── ARCHITECTURE.md
│   └── DATA_PRESENTATION.md
│
└── .github/workflows/
    └── ci.yml               # CI pipeline (tests automation)
```

### Dataset

The project uses a synthetic e-commerce dataset stored in AWS S3.

#### Source

* Data is stored in an S3 bucket
* Accessed using AWS credentials defined in the `.env` file
* Prefix: `raw/`

---

#### Data Domains

The dataset is composed of multiple interconnected tables representing an online retail system:

##### Core business data

* **products**
  Product catalog including price, category, and tags

* **users**
  Customer information (cleaned in Silver to remove sensitive data)

* **orders**
  Customer orders with status, timestamps, and totals

* **order_line_items**
  Detailed breakdown of each order (products, quantities, prices)

* **payments**
  Payment transactions linked to orders

---

##### Operational and behavioral data

* **inventory**
  Stock levels and product availability

* **marketing**
  Campaign and acquisition data

* **search_events**
  User search activity on the platform

* **abandoned_carts**
  Carts not converted into purchases

* **reviews**
  Customer feedback and ratings

* **interactions**
  User interactions with the platform

* **clickstream**
  Navigation and browsing behavior

---

#### Data Usage in the Pipeline

* Bronze layer:

  * stores raw data as ingested from S3
  * no transformation applied

* Silver layer:

  * cleans and standardizes each dataset
  * enforces data quality rules
  * removes invalid or inconsistent records

* Gold layer:

  * aggregates data into business metrics
  * examples:

    * daily revenue
    * product performance
    * customer lifetime value

---

#### Key Characteristics

* Multi-table relational dataset
* Mix of transactional and behavioral data
* Designed to simulate real-world e-commerce analytics use cases

---

## 3. Setup Instructions

### Clone the repository

    git clone <repo-url>
    cd imt-elt-coding-master

### Create a virtual environment

    python -m venv venv
    source venv/bin/activate

On Windows:

    venv\Scripts\activate

### Install dependencies

    pip install -r requirements.txt

### Configure environment variables

Create a `.env` file based on the following template:

    # PostgreSQL (AWS RDS)

    RDS_HOST=your-rds-host
    RDS_PORT=5432
    RDS_DATABASE=your-database-name
    RDS_USER=your-username
    RDS_PASSWORD=your-password

    # Schema names (replace X with your group number)

    BRONZE_SCHEMA=bronze_groupX
    SILVER_SCHEMA=silver_groupX
    GOLD_SCHEMA=gold_groupX

    # AWS S3 (read-only — raw data)

    S3_BUCKET_NAME=your-bucket-name
    S3_PREFIX=raw
    AWS_REGION=your-region

    AWS_ACCESS_KEY_ID=your-access-key
    AWS_SECRET_ACCESS_KEY=your-secret-key


### Test database connection

    python -m src.database

This command validates:
- database connectivity  
- environment variables configuration  
- schema availability  

---

## 4. How to Run

### Run the full pipeline

    python pipeline.py

### Run individual steps

    python pipeline.py --step extract
    python pipeline.py --step transform
    python pipeline.py --step gold

The pipeline executes:
1. Extraction into Bronze  
2. Transformation into Silver  
3. Gold aggregation  

At the end of execution, a report is generated:

    pipeline_report.json

---

## 5. How to Test

Tests are implemented using `pytest`.

### Run all tests

```
pytest tests/ -v --cov=src --cov-report=html
pytest tests/ -v --cov=src --cov-report=term-missing
```

These commands:

* run all tests in the `tests/` directory
* enable verbose output (`-v`) to display detailed test results
* measure code coverage on the `src/` module

The first command:

* generates a full HTML coverage report
* creates a folder `htmlcov/` with a detailed visualization of coverage

The second command:

* displays missing coverage directly in the terminal
* highlights which lines are not covered by tests (`term-missing`)

### View coverage report

```
    htmlcov/index.html
    in Powershell: Start-Process htmlcov/index.html
```

### Test scope

- Extract layer: verifies data extraction functions and Bronze loading  
- Transform layer: validates cleaning rules and data consistency  
- Gold layer: checks aggregation logic and table creation  
- Tests rely on mocking to isolate logic from external systems  

### Database testing

The real database connection is not tested via pytest.

Instead, it is validated separately with:

    python -m src.database

---

## 6. Team Members

| Name | Role |
|------|------|
| ARAÚJO COSTA João | Data processing and quality, Monitoring |
| CÁCERES Alejandro | Database and infrastructure, Testing & CI |
| FLICHY Astrid | Pipeline development, Documentation |

---

## Additional Notes

The project follows key data engineering principles:
- layered architecture (Bronze / Silver / Gold)  
- modular design  
- separation of concerns  
- reproducibility  
- testability with pytest  
- monitoring via execution reports
- automated CI pipeline via GitHub Actions that runs pytest and enforces code quality  

### Possible Improvements

- improve pipeline orchestration  
- add integration tests with a real database  
- enhance logging and observability  
- externalize configuration management
- refacturation functions 
