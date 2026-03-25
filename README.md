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

### Possible Improvements

- improve pipeline orchestration  
- add integration tests with a real database  
- enhance logging and observability  
- externalize configuration management  
