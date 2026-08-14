# Data Engineering Lab: ETL Data Ingestion Pipeline

## Project Overview

This repository contains a **production-ready ETL (Extract-Transform-Load) data ingestion pipeline** built with Apache Airflow and PostgreSQL. The project demonstrates core data engineering concepts including multi-source extraction, data quality transformations, idempotent loading, and workflow orchestration.

This represents **Modules 1–15 of a comprehensive Data Engineering lab**, with Module 16 dedicated to Dockerization.

---

## Project Objective

Build an automated, maintainable ETL pipeline that:

1. **Extracts** data from multiple sources (CSV files, JSON files, REST APIs)
2. **Transforms** raw data through cleaning, validation, and business rule enforcement
3. **Loads** processed data into PostgreSQL with idempotent UPSERT semantics
4. **Orchestrates** the complete workflow using Apache Airflow
5. **Monitors** execution through logging, error handling, and Airflow UI

---

## Problem Statement

Modern data platforms must ingest data from heterogeneous sources while ensuring:

- **Data Quality**: Clean, validated, consistent data
- **Idempotency**: Safe re-runs without duplicates or data loss
- **Observability**: Full audit trails and execution logs
- **Reliability**: Automatic retries and failure handling
- **Scalability**: Design ready for containerization and cloud deployment

This pipeline addresses these requirements systematically.

---

## Key Features

✅ **Multi-Source Extraction**
- CSV file extraction with Pandas
- JSON file parsing with nested record support
- REST API calls with retry logic and error handling

✅ **Intelligent Data Transformation**
- Column name standardization
- Data type validation and conversion
- Duplicate removal (exact and near-duplicate detection)
- Email validation and normalization
- Business rule enforcement (e.g., allowed product categories)
- Foreign key mapping for relational integrity

✅ **Idempotent Loading**
- PostgreSQL UPSERT implementation
- Automatic conflict detection and handling
- Transaction-safe operations with rollback
- Support for insert-only and update-on-conflict scenarios

✅ **Airflow Orchestration**
- Task-based parallelization (3 concurrent data streams)
- Automatic retry with exponential backoff
- Dependency management and task sequencing
- Daily scheduling with catchup disabled

✅ **Production-Grade Logging**
- Console output (INFO level) for operators
- File logging (DEBUG level) for troubleshooting
- Structured log messages with context
- Per-module logger configuration

✅ **Error Handling & Recovery**
- Try-catch blocks with detailed error messages
- Input validation and schema checking
- Graceful handling of missing files or API failures
- Configurable timeouts and retry policies

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Apache Airflow Scheduler                     │
│                                                                 │
│  DAG: etl_ingestion_pipeline (Daily)                           │
└────────────────────┬────────────────────────────────────────────┘
                     │
          ┌──────────┴──────────┐
          │                     │
    ┌─────▼──────┐        ┌────▼──────┐
    │  Extraction│        │Orchestration
    │   Layer    │        │   Layer
    └─────┬──────┘        └────┬───────┘
          │                    │
    ┌─────▼─────────────────┐  │
    │  3 Parallel Streams:  │  │
    │  • CSV (Customers)    │  │
    │  • JSON (Products)    │  │
    │  • API (Orders)       │  │
    └─────┬─────────────────┘  │
          │                    │
    ┌─────▼──────────────┐     │
    │Transformation Layer│     │
    │ • Cleaning         │     │
    │ • Validation       │     │
    │ • Business Rules   │     │
    └─────┬──────────────┘     │
          │                    │
    ┌─────▼──────────────┐     │
    │ Loading Layer      │     │
    │ • UPSERT Logic     │     │
    │ • Idempotency      │     │
    │ • Transactions     │     │
    └─────┬──────────────┘     │
          │                    │
    ┌─────▼──────────────────────────┐
    │    PostgreSQL Database         │
    │  ┌──────────────────────────┐  │
    │  │  customers (101 rows)    │  │
    │  │  products  (81 rows)     │  │
    │  │  orders    (100 rows)    │  │
    │  └──────────────────────────┘  │
    └────────────────────────────────┘
```

---

## End-to-End Data Flow

### Customers Pipeline
```
data/raw/customers.csv
    ↓
[CSV Extractor] → Pandas DataFrame
    ↓
[Customer Transformer]
  • Standardize column names
  • Remove duplicates
  • Validate emails
  • Title case names
    ↓
[DB Loader: UPSERT]
  • Use customer_id as primary key
  • Insert new or update existing
    ↓
PostgreSQL: customers table
```

### Products Pipeline
```
data/raw/products.json
    ↓
[JSON Extractor] → Pandas DataFrame
    ↓
[Product Transformer]
  • Standardize column names
  • Deduplicate by product_id
  • Validate category
  • Enforce allowed categories
    ↓
[DB Loader: UPSERT]
  • Use product_id as primary key
    ↓
PostgreSQL: products table
```

### Orders Pipeline
```
https://jsonplaceholder.typicode.com/posts
    ↓
[API Extractor]
  • Retry logic (3 attempts)
  • HTTP status handling
  • JSON deserialization
    ↓
[Order Transformer]
  • Map API userId → customer_id
  • Standardize column names
  • Remove missing foreign keys
  • Add quantity (default: 1)
    ↓
[DB Loader: UPSERT]
  • Use order_id as primary key
    ↓
PostgreSQL: orders table
```

---

## Data Sources

### 1. CSV: Customers
- **Location**: `data/raw/customers.csv`
- **Format**: CSV with UTF-8 encoding
- **Columns**: customer_id, first_name, last_name, email, phone, country, signup_date, annual_spend
- **Characteristics**: Flat structure, occasional duplicates, requires email validation

### 2. JSON: Products
- **Location**: `data/raw/products.json`
- **Format**: JSON array
- **Columns**: product_id, name, category, price, stock, rating, created_at
- **Characteristics**: Normalized structure, business rule on categories (electronics, kitchen, clothing, home, books, sports)

### 3. REST API: Orders
- **Endpoint**: `https://jsonplaceholder.typicode.com/posts`
- **Format**: JSON array of objects
- **Note**: JSONPlaceholder is a mock API; userId values (1-10) are mapped to actual customer_id values
- **Columns**: id (→order_id), userId (→customer_id), title, body
- **Characteristics**: External API, requires retry logic, synthetic data for lab

---

## Extraction Layer

### CSV Extractor (`pipeline/extractors/csv_extractor.py`)

Loads CSV files using Pandas with:
- UTF-8 encoding by default
- File existence validation
- Shape logging (rows × columns)
- Exception re-raising for upstream handling

**Example**:
```python
df = extract_csv("data/raw/customers.csv")
# Returns: DataFrame with raw CSV data
```

### JSON Extractor (`pipeline/extractors/json_extractor.py`)

Parses JSON files with:
- JSON schema validation
- Support for nested records (via `record_path` parameter)
- Normalization to DataFrame
- File existence and decode error handling

**Example**:
```python
df = extract_json("data/raw/products.json")
# Returns: DataFrame with normalized JSON data
```

### API Extractor (`pipeline/extractors/api_extractor.py`)

Fetches data from REST APIs with:
- Automatic retry logic (3 attempts, exponential backoff)
- HTTP status code handling (429, 500, 502, 503, 504)
- Configurable timeout (default: 10 seconds)
- Session-based connection pooling
- Elapsed time logging

**Example**:
```python
df = extract_api("https://jsonplaceholder.typicode.com/posts", timeout=10)
# Returns: DataFrame from JSON API response
```

---

## Transformation / Cleaning Layer

### Customer Transformer (`pipeline/transformers/customer_transformer.py`)

Cleans customer data via:
1. **Column normalization**: Strip, lowercase, underscores (e.g., "First Name" → "first_name")
2. **Deduplication**: Remove exact duplicates
3. **Name cleaning**: Title case first/last names, trim whitespace
4. **Email validation**: Regex pattern matching, invalid emails → NULL
5. **Phone normalization**: Trim whitespace
6. **Date parsing**: ISO format validation

**Quality Metrics**: Tracks duplicates removed, invalid emails detected, rows cleaned

### Product Transformer (`pipeline/transformers/product_transformer.py`)

Validates product data via:
1. **Column normalization**: Lowercase with underscores
2. **ID deduplication**: Keep first occurrence of duplicate product_id
3. **Category enforcement**: Drop rows with missing or invalid categories
4. **Allowed categories**: {electronics, kitchen, clothing, home, books, sports}
5. **Price validation**: Numeric conversion with error handling
6. **Stock/rating normalization**: Numeric types

**Quality Metrics**: Tracks duplicates, invalid categories, data type conversions

### Order Transformer (`pipeline/transformers/order_transformer.py`)

Transforms API data to database schema via:
1. **Field mapping**: API field names → database columns (id → order_id, userId → customer_id)
2. **ID mapping**: JSONPlaceholder userId (1-10) → actual customer_id (deterministic lab mapping)
3. **Column subset**: Keep only required fields
4. **Foreign key filtering**: Drop orders with missing customer_id or order_id
5. **Synthetic quantity**: Default to 1 (JSONPlaceholder does not provide quantity)

**Quality Metrics**: Tracks removed records, ID mapping results

---

## PostgreSQL Database Layer

### Schema Design

**Table: customers**
```sql
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    country VARCHAR(10),
    signup_date TIMESTAMP,
    annual_spend FLOAT
);
```

**Table: products**
```sql
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    category VARCHAR(50) NOT NULL,
    price FLOAT NOT NULL,
    stock INTEGER,
    rating FLOAT,
    created_at TIMESTAMP
);
```

**Table: orders**
```sql
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL FOREIGN KEY REFERENCES customers(customer_id),
    title VARCHAR(255) NOT NULL,
    body TEXT,
    quantity INTEGER NOT NULL DEFAULT 1,
    order_date TIMESTAMP NOT NULL
);
```

### Constraints
- **customers**: Primary key (customer_id), unique email
- **products**: Primary key (product_id)
- **orders**: Primary key (order_id), foreign key to customers(customer_id)

---

## Loading Strategy

### UPSERT Implementation

The loader uses PostgreSQL's `ON CONFLICT DO UPDATE` clause to implement **idempotent loading**:

```sql
INSERT INTO customers (...) VALUES (...)
ON CONFLICT (customer_id) DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    email = EXCLUDED.email,
    ...
```

**Benefits**:
- **Idempotency**: Re-running the pipeline produces identical results
- **No duplicates**: Primary key conflicts automatically trigger updates
- **Transaction safety**: All-or-nothing semantics with rollback on error
- **Atomicity**: Multi-row operations complete as a single transaction

### Loading Flow (`pipeline/loaders/db_loader.py`)

1. **Record preparation**: Convert Pandas DataFrame to dict list
   - Handle NaN/NaT/pd.NA conversion to Python None
   - Type preservation for database driver compatibility

2. **UPSERT execution**: Execute INSERT ... ON CONFLICT statement
   - Identify primary key and non-key columns
   - Build UPDATE dictionary excluding primary keys
   - Execute transaction

3. **Error handling**: Rollback on exception, log error details

4. **Logging**: Track row counts, timing, and status

---

## UPSERT / Idempotency

### Why Idempotency Matters

In a production pipeline:
- Networks fail (API timeouts, database connectivity)
- Scheduled jobs may retry on partial completion
- Operators may manually re-run failed DAG runs
- Data correction updates must be reapplied

**Without idempotency**: Re-runs create duplicates and data inconsistency
**With idempotency**: Re-runs are safe; the final state is always correct

### How It Works Here

1. **Primary Key as Identity**: Each table has a primary key (customer_id, product_id, order_id)
2. **Conflict Detection**: PostgreSQL detects when an insert violates a primary key constraint
3. **Automatic Update**: Instead of failing, the UPSERT updates all non-key columns
4. **Transactional Guarantee**: The entire batch succeeds or rolls back

### Example Scenario

**First run**:
```
INSERT INTO customers VALUES (101, 'John', 'Doe', ...)
→ 1 new row inserted
```

**Second run** (re-execution):
```
INSERT INTO customers VALUES (101, 'John', 'Doe', ...) [same data]
→ Conflict on customer_id=101
→ UPDATE all columns to same values
→ Same result: customer_id=101 exists
```

**Third run** (data correction):
```
INSERT INTO customers VALUES (101, 'Jane', 'Smith', ...) [updated name]
→ Conflict on customer_id=101
→ UPDATE first_name='Jane', last_name='Smith'
→ Same customer_id=101, new data applied
```

---

## Airflow Orchestration

### DAG: `etl_ingestion_pipeline`

**Configuration**:
- **Schedule**: `@daily` (every day at 00:00 UTC)
- **Start Date**: 2026-01-01
- **Catchup**: Disabled (no backfill of historical runs)
- **Retries**: 2 (automatic retry on task failure)
- **Retry Delay**: 5 minutes between retries
- **Owner**: data_eng
- **Tags**: ['ingestion', 'etl']

### Task Dependency Flow

```
initialize_db
    │
    ├──→ process_customers ─────┐
    │                            │
    ├──→ process_products ───────┼──→ summarize
    │                            │
    └──→ process_orders ─────────┘
```

**Explanation**:
1. **initialize_db**: Runs first, creates database schema if needed
2. **process_customers**, **process_products**, **process_orders**: Run in parallel after init
3. **summarize**: Runs after all three processes complete

### DAG Information

| Property | Value |
|----------|-------|
| DAG ID | etl_ingestion_pipeline |
| Schedule | @daily (00:00 UTC) |
| Timezone | UTC |
| Executor | SequentialExecutor (local execution) |
| Parallelism | 32 tasks max across all DAGs |
| Max Active Tasks/DAG | 16 tasks |
| Max Active Runs/DAG | 16 concurrent runs |
| Paused at Creation | True (unpause to enable scheduling) |

### Task Information

| Task | Type | Timeout | Retries | Description |
|------|------|---------|---------|-------------|
| initialize_db | PythonOperator | 300s | 2 | Create database schema |
| process_customers | PythonOperator | 300s | 2 | Extract, transform, load customers |
| process_products | PythonOperator | 300s | 2 | Extract, transform, load products |
| process_orders | PythonOperator | 300s | 2 | Extract, transform, load orders |
| summarize | PythonOperator | 300s | 2 | Print execution summary |

---

## Task-by-Task Explanation

### 1. Task: `initialize_db`

**Purpose**: Create all database tables if they do not exist

**Implementation**:
```python
@task
def initialize_db():
    from pipeline.loaders.db_connection import init_db
    init_db()
    return True
```

**Execution**:
- Calls `pipeline.loaders.db_connection.init_db()`
- Uses SQLAlchemy `Base.metadata.create_all(engine)`
- Idempotent: safe to run multiple times
- Logs success/failure

**Output**: True (success) or exception

### 2. Task: `process_customers`

**Purpose**: Extract, transform, load customer data

**Dependency**: Requires `initialize_db` to complete first

**Execution Steps**:
1. Extract CSV from `data/raw/customers.csv`
2. Transform via `transform_customers()`:
   - Column name standardization
   - Remove duplicates
   - Validate emails
3. Load via `load_customers()`:
   - UPSERT into customers table
   - Use customer_id as conflict key
   - Update non-key columns

**Output**:
```python
{
    "raw": <num_rows_extracted>,
    "clean": <num_rows_after_transformation>,
    "loaded": <num_rows_upserted>
}
```

**Example**:
```python
{"raw": 100, "clean": 98, "loaded": 98}  # 2 duplicates removed
```

### 3. Task: `process_products`

**Purpose**: Extract, transform, load product data

**Dependency**: Requires `initialize_db` to complete first

**Execution Steps**:
1. Extract JSON from `data/raw/products.json`
2. Transform via `transform_products()`:
   - Column name standardization
   - Deduplicate by product_id
   - Validate categories against allowed list
3. Load via `load_products()`:
   - UPSERT into products table
   - Use product_id as conflict key

**Output**:
```python
{
    "raw": <num_rows_extracted>,
    "clean": <num_rows_after_transformation>,
    "loaded": <num_rows_upserted>
}
```

### 4. Task: `process_orders`

**Purpose**: Extract, transform, load order data

**Dependency**: Requires `initialize_db` to complete first

**Execution Steps**:
1. Extract from JSONPlaceholder API
2. Transform via `transform_orders()`:
   - Map API userId → customer_id
   - Keep required fields
   - Remove records with missing foreign keys
   - Add synthetic quantity
3. Load via `load_orders()`:
   - UPSERT into orders table
   - Use order_id as conflict key

**Output**:
```python
{
    "raw": <num_rows_extracted>,
    "clean": <num_rows_after_transformation>,
    "loaded": <num_rows_upserted>
}
```

### 5. Task: `summarize`

**Purpose**: Print execution summary to logs

**Dependency**: Requires all three process_* tasks to complete

**Execution**:
- Receives output from process_customers, process_products, process_orders
- Prints formatted summary table

**Output**:
```
============================================================
AIRFLOW PIPELINE SUMMARY
============================================================
Customers: {'raw': 100, 'clean': 98, 'loaded': 98}
Products:  {'raw': 81, 'clean': 81, 'loaded': 81}
Orders:    {'raw': 100, 'clean': 100, 'loaded': 100}
============================================================
```

---

## Logging and Error Handling

### Logging Configuration

**Logger Setup** (`pipeline/utils/logger.py`):

```python
logging.basicConfig(
    level=logging.DEBUG,
    handlers=[console_handler, file_handler]
)
```

- **Console**: INFO and above (user-facing output)
- **File**: DEBUG and above (detailed troubleshooting)
- **File Location**: `logs/pipeline.log`
- **Format**: `%(asctime)s [%(levelname)s] %(name)s: %(message)s`

### Log Levels

| Level | Usage | Example |
|-------|-------|---------|
| DEBUG | Detailed flow information | "Processing row 42 of 100" |
| INFO | Significant events | "Starting customer pipeline" |
| WARNING | Unexpected but recoverable | "Products removed for missing category" |
| ERROR | Failure requiring attention | "Failed to parse CSV: Invalid encoding" |
| CRITICAL | System failure | "Database connection lost" |

### Error Handling Strategy

1. **Extraction errors**: File not found, API timeout, invalid JSON
   - → Log error details
   - → Re-raise to Airflow
   - → Trigger automatic retry

2. **Transformation errors**: Data validation, type conversion
   - → Log row-level details
   - → Remove problematic rows (or raise depending on severity)
   - → Continue pipeline

3. **Loading errors**: Database constraint violation, connection failure
   - → Log error and full traceback
   - → Rollback transaction
   - → Re-raise to Airflow

### Example Log Output

```
2026-08-14 22:28:23 [INFO] pipeline.extractors.csv_extractor: Extracted 100 rows and 8 columns from CSV: data/raw/customers.csv
2026-08-14 22:28:24 [INFO] pipeline.transformers.customer_transformer: Starting customer transformation | input_rows=100
2026-08-14 22:28:24 [INFO] pipeline.transformers.customer_transformer: Customer duplicates removed | count=2
2026-08-14 22:28:25 [INFO] pipeline.loaders.db_loader: Loading customers | rows=98
2026-08-14 22:28:26 [INFO] pipeline.loaders.db_loader: UPSERT completed | table=customers | rows=98
2026-08-14 22:28:26 [INFO] dag_utils: Customer pipeline completed | {'raw': 100, 'clean': 98, 'loaded': 98}
```

---

## Project Structure

```
01_lab_data_ingestion_pipeline/
│
├── README.md ............................ This file (project overview)
├── SETUP.md ............................. Setup and operations guide
│
├── airflow.cfg .......................... Airflow configuration
├── airflow.db ........................... Airflow metadata database (auto-created)
├── webserver_config.py .................. Airflow webserver settings
│
├── .env ................................ Environment variables (GITIGNORED)
├── .env.example ......................... Template for .env file
├── .gitignore ........................... Git exclusions
│
├── requirements.txt ..................... Python dependencies
├── main.py ............................. Entry point (optional, for local testing)
│
├── dags/
│   ├── __init__.py
│   ├── etl_ingestion_dag.py ............ Main Airflow DAG definition
│   └── dag_utils.py ................... Reusable pipeline functions
│
├── pipeline/
│   ├── __init__.py
│   ├── config.py ....................... Configuration and database URL
│   │
│   ├── extractors/
│   │   ├── __init__.py
│   │   ├── csv_extractor.py ........... Extract from CSV files
│   │   ├── json_extractor.py ......... Extract from JSON files
│   │   └── api_extractor.py ......... Extract from REST APIs
│   │
│   ├── transformers/
│   │   ├── __init__.py
│   │   ├── customer_transformer.py ... Clean customer data
│   │   ├── product_transformer.py ... Validate product data
│   │   └── order_transformer.py ..... Transform order data
│   │
│   ├── loaders/
│   │   ├── __init__.py
│   │   ├── db_connection.py ......... SQLAlchemy ORM models & engine
│   │   └── db_loader.py ............ UPSERT logic
│   │
│   └── utils/
│       ├── __init__.py
│       └── logger.py ................. Logging configuration
│
├── data/
│   ├── raw/ ........................... Input files (CSV, JSON)
│   │   ├── customers.csv
│   │   └── products.json
│   │
│   └── processed/ ..................... Processed data (if needed)
│
├── logs/
│   ├── pipeline.log ................... Application logs
│   │
│   ├── dag_id=etl_ingestion_pipeline/
│   │   ├── run_id=manual__2026-08-12T215349.792649+0000/
│   │   │   ├── task_id=initialize_db/
│   │   │   ├── task_id=process_customers/
│   │   │   ├── task_id=process_products/
│   │   │   ├── task_id=process_orders/
│   │   │   └── task_id=summarize/
│   │   └── ...
│   │
│   ├── scheduler/ ..................... Airflow scheduler logs
│   └── dag_processor_manager/ ......... Airflow DAG parser logs
│
├── reports/
│   └── project_technical_report.md ... Module 15 validation & Module 16 planning
│
├── airflow_venv/ ...................... Python virtual environment
│   └── lib/python3.12/site-packages/  ... Installed dependencies
│
├── venv/ ............................. Alternative virtual environment location
│
└── standalone_admin_password.txt ...... Airflow UI login password (GITIGNORED)
```

### Key Directories

| Directory | Purpose |
|-----------|---------|
| `dags/` | Airflow DAG definitions and utilities |
| `pipeline/` | Core ETL logic (extractors, transformers, loaders) |
| `data/raw/` | Input data files (CSV, JSON) |
| `data/processed/` | Processed data outputs |
| `logs/` | Execution logs (Airflow + application) |
| `reports/` | Technical documentation and reports |

### Sensitive Files (Git Ignored)

These files should NEVER be committed:
- `.env` – Database credentials, API keys, secrets
- `.env.*` – Environment variable overrides
- `airflow.db` – Airflow metadata database (contains user credentials)
- `standalone_admin_password.txt` – Airflow UI login password
- `airflow-webserver.pid` – Process IDs
- `logs/` – Execution logs (may contain sensitive data)
- `*venv/` – Virtual environment with installed packages
- `__pycache__/` – Python bytecode cache

---

## Technology Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| Python | 3.12+ | Language runtime |
| Apache Airflow | 2.10.2 | Workflow orchestration |
| PostgreSQL | 13+ (external) | Data warehouse |
| SQLAlchemy | Included | ORM for database access |
| Pandas | Included | Data manipulation and transformation |
| Requests | Included | HTTP client for API calls |
| Python-dotenv | Included | Environment variable management |
| psycopg2 | Included | PostgreSQL database driver |

---

## Environment Configuration

### Environment Variables

Create a `.env` file (based on `.env.example`) with:

```bash
# PostgreSQL - Pipeline Database
DB_USER=pipeline_user
DB_PASS=<your_secure_password>
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ingestion_db

# PostgreSQL - Airflow Metadata Database
AIRFLOW_DB_USER=airflow_user
AIRFLOW_DB_PASS=<your_secure_password>
AIRFLOW_DB_HOST=localhost
AIRFLOW_DB_PORT=5432
AIRFLOW_DB_NAME=airflow_db

# Database URLs (for SQLAlchemy)
DATABASE_URL=postgresql://pipeline_user:password@localhost:5432/ingestion_db
AIRFLOW_DB_URL=postgresql://airflow_user:password@localhost:5432/airflow_db

# External API
API_BASE_URL=https://jsonplaceholder.typicode.com
API_TIMEOUT=10

# Logging
LOG_LEVEL=INFO
```

### Configuration Files

- **`airflow.cfg`**: Airflow core settings (dags_folder, executor, etc.)
- **`webserver_config.py`**: Airflow web UI customization
- **`pipeline/config.py`**: Application settings (loaded from .env)

---

## Local Setup

Quick reference for getting started. See [SETUP.md](SETUP.md) for detailed instructions.

### Prerequisites
- Python 3.12+
- PostgreSQL 13+ (local or remote)
- Git
- Windows PowerShell or WSL2 bash

### Quick Start

```bash
# 1. Clone and enter repository
git clone <repo_url>
cd 01_lab_data_ingestion_pipeline

# 2. Create Python virtual environment
python -m venv airflow_venv

# 3. Activate virtual environment
# Windows PowerShell:
.\airflow_venv\Scripts\Activate.ps1
# WSL2 bash:
source airflow_venv/bin/activate

# 4. Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# 5. Configure environment variables
cp .env.example .env
# Edit .env with your PostgreSQL credentials

# 6. Set up databases
createdb airflow_db
createdb ingestion_db

# 7. Initialize Airflow
airflow db migrate
airflow users create --username admin --password admin --role Admin

# 8. Start Airflow (standalone mode)
airflow standalone

# 9. Open Airflow UI
# http://localhost:8080
```

For complete instructions, see [SETUP.md](SETUP.md).

---

## Running the Pipeline

### Via Airflow UI

1. **Start Airflow**: `airflow standalone`
2. **Open UI**: http://localhost:8080
3. **Login**: Username/password from setup
4. **Find DAG**: Search for "etl_ingestion_pipeline"
5. **Unpause DAG**: Click toggle (if paused)
6. **Trigger Run**: Click "Trigger DAG"
7. **Monitor**: Watch task execution in Graph/Tree view

### Via Command Line

```bash
# Trigger a manual DAG run
airflow dags trigger etl_ingestion_pipeline

# List all DAG runs
airflow dags list-runs -d etl_ingestion_pipeline

# Get task logs
airflow tasks logs etl_ingestion_pipeline <task_name> <logical_date>

# Example:
airflow tasks logs etl_ingestion_pipeline process_customers 2026-08-14
```

---

## Module 15 Validation

### Successful Execution Baseline

All five tasks in `etl_ingestion_pipeline` have been successfully executed:

✅ **Task: initialize_db**
- Status: SUCCESS
- Action: Created database schema (3 tables)
- Idempotent: Yes (safe to re-run)

✅ **Task: process_customers**
- Status: SUCCESS
- Rows: Extracted 100 → Cleaned 98 → Loaded 98
- Duplicates removed: 2
- Unique emails validated

✅ **Task: process_products**
- Status: SUCCESS
- Rows: Extracted 81 → Cleaned 81 → Loaded 81
- Category validation: All rows passed
- No duplicates detected

✅ **Task: process_orders**
- Status: SUCCESS
- Rows: Extracted 100 → Cleaned 100 → Loaded 100
- API mapping: userId (1-10) → customer_id
- Foreign keys: All valid

✅ **Task: summarize**
- Status: SUCCESS
- Output: Printed summary of all three data streams
- Confirmation: All tasks completed successfully

### Verification Evidence

- Airflow Graph View: All 5 tasks shown as SUCCESS (green)
- Airflow Tree View: All runs completed without retries
- PostgreSQL: 3 tables with correct row counts and data
- Logs: No errors or warnings in pipeline execution

### What This Means

**Module 15 represents a stable, working data pipeline** that:
- Extracts from multiple sources reliably
- Transforms data correctly with business rule enforcement
- Loads data idempotently into PostgreSQL
- Orchestrates complex workflows via Airflow
- Logs execution for monitoring and troubleshooting

**Module 16 Dockerization will preserve this functionality** while adding containerization, scaling, and deployment capabilities.

---

## Module 16: Dockerization Objective

### Purpose

Module 16 extends Module 15 by containerizing the entire pipeline:

1. **Containerize Airflow**: Run Airflow inside Docker
2. **Containerize PostgreSQL**: Run database inside Docker
3. **Use Docker Compose**: Orchestrate multi-container setup
4. **Mount source code**: Bind host directories into containers
5. **Manage environment**: Use Docker Compose env file

### Intended Architecture

```
┌─────────────────────────────────────────────────────────┐
│              Docker Compose Network                     │
│                                                         │
│  ┌──────────────────────┐    ┌──────────────────────┐ │
│  │  Airflow Container   │    │ PostgreSQL Container │ │
│  │                      │    │                      │ │
│  │  • Airflow Scheduler │    │ • airflow_db        │ │
│  │  • Airflow Webserver │    │ • ingestion_db      │ │
│  │  • Python 3.12       │    │ • Port: 5432        │ │
│  │  • Port: 8080        │ ↔→ │                      │ │
│  │                      │    └──────────────────────┘ │
│  │ Volumes:             │                             │
│  │  • dags/             │                             │
│  │  • pipeline/         │                             │
│  │  • logs/             │                             │
│  │  • data/             │                             │
│  └──────────────────────┘                             │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Key Docker Concepts

**Image**: Template for containers (like a class)
- Base image: `apache/airflow:latest-python3.12`
- Includes: Python, Airflow, dependencies

**Container**: Running instance of an image (like an object)
- Isolated filesystem, processes, networking
- Can be started, stopped, restarted

**Volume**: Persistent storage outside container
- Host files visible inside container (bind mount)
- Data persists after container stops

**Compose Service**: Named container managed by Docker Compose
- Service name: `airflow`, `postgres`
- Can reference by name (not localhost)

**Network**: Bridge between containers
- Services communicate by service name
- Airflow connects to `postgres:5432` (not `localhost:5432`)

### Critical Docker-to-Database Connection

In **local execution**:
```python
DATABASE_URL = "postgresql://user:pass@localhost:5432/ingestion_db"
```

In **Docker execution**:
```python
DATABASE_URL = "postgresql://user:pass@postgres:5432/ingestion_db"
# Note: "postgres" is the Docker Compose service name
```

This is the single most important difference when Dockerizing.

### Status: Planned for Module 16

**Current state**: Module 15 baseline is fully functional locally
**Docker status**: NOT YET IMPLEMENTED

The following Docker artifacts will be created in Module 16:
- `Dockerfile` (Airflow image specification)
- `docker-compose.yml` (Multi-container orchestration)
- `.dockerignore` (Files to exclude from build context)
- `docker/.env` (Docker-specific environment variables)
- Docker-specific SETUP instructions

**No Docker files currently exist in this repository.**

---

## Planned Docker Architecture

When Module 16 is implemented, the pipeline will run inside Docker Compose with:

1. **Airflow Service**
   - Image: apache/airflow:2.10.2-python3.12
   - Ports: 8080 (webserver)
   - Volumes: dags/, pipeline/, logs/, data/
   - Environment: DATABASE_URL points to postgres:5432

2. **PostgreSQL Service**
   - Image: postgres:15-alpine
   - Ports: 5432 (local access for testing)
   - Volumes: postgres_data/ (persistent storage)
   - Environment: POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB

3. **Docker Compose Network**
   - Services communicate by name (postgres service from airflow)
   - Isolated from host system (except exposed ports)

4. **Volume Strategy**
   - Source code mounted as bind volumes (read-only)
   - Logs/data stored in named volumes (persistent)
   - Database stored in named volume (persists between container restarts)

### Benefits of Dockerization

✅ **Consistency**: Same environment on all machines (laptop, CI/CD, production)
✅ **Isolation**: No conflicts between Python versions, Airflow versions
✅ **Scalability**: Easily add more containers (workers, replicas)
✅ **Portability**: Works on Windows, Mac, Linux, cloud platforms
✅ **Reproducibility**: Docker Compose config is version-controlled
✅ **Development**: Use same Docker setup for local development and production

---

## Future Enhancements

### Planned Improvements (Beyond Module 16)

1. **Incremental Processing**
   - Add "last_run_timestamp" tracking
   - Process only new records since last execution
   - Reduce data transfer and processing time

2. **Data Quality Checks**
   - Implement Great Expectations framework
   - Add schema validation before loading
   - Generate quality reports

3. **Performance Optimization**
   - Batch processing with configurable chunk size
   - Parallel task execution in Kubernetes
   - Connection pooling optimization

4. **Observability**
   - Prometheus metrics export
   - Grafana dashboard for pipeline monitoring
   - Alert rules (task failure, SLA breach)

5. **Cloud Deployment**
   - AWS ECS / Fargate
   - Google Cloud Composer (Managed Airflow)
   - Azure Container Instances
   - Kubernetes deployment manifest

6. **Advanced Data Transformations**
   - PySpark for larger datasets
   - Incremental fact/dimension loading
   - Complex business logic (SCD Type 2)

7. **Infrastructure as Code**
   - Terraform for cloud infrastructure
   - Helm charts for Kubernetes
   - CloudFormation for AWS

---

## Learning Outcomes

Upon completing Modules 1–15, you should understand:

### 1. ETL Fundamentals
- How to design extraction, transformation, and loading phases
- Trade-offs between different data sources
- Data quality principles and validation techniques

### 2. Data Engineering
- Working with CSV, JSON, and REST API data sources
- Pandas for data manipulation and cleaning
- RDBMS design and SQL fundamentals

### 3. Database Management
- PostgreSQL schema design and constraints
- Transaction management and atomicity
- UPSERT patterns for idempotent operations
- SQLAlchemy ORM for Python database access

### 4. Workflow Orchestration
- Airflow DAG design and task dependencies
- Scheduling and monitoring
- Error handling and retry logic
- Logging for observability

### 5. Python Development
- Virtual environments and dependency management
- Module organization and imports
- Exception handling and logging
- Configuration management with environment variables

### 6. DevOps Fundamentals
- Git version control and .gitignore
- Secrets management and security
- Development vs. production environments
- Infrastructure and deployment concepts (ready for Module 16)

---

## Troubleshooting

See [SETUP.md](SETUP.md) for detailed troubleshooting steps. Common issues:

| Issue | Cause | Fix |
|-------|-------|-----|
| "ModuleNotFoundError: pipeline" | PYTHONPATH not set | Add project root to PYTHONPATH |
| "psycopg2.OperationalError: connection refused" | PostgreSQL not running | Start PostgreSQL service |
| "DAG not appearing in Airflow" | DAGS_FOLDER not configured | Check airflow.cfg, restart scheduler |
| "Airflow import errors" | Dependency missing | `pip install -r requirements.txt` |
| ".env not found" | Config file missing | Copy .env.example to .env |

For complete troubleshooting guide, see [SETUP.md](SETUP.md).

---

## Additional Resources

### Documentation Files
- [SETUP.md](SETUP.md) – Step-by-step setup guide
- [reports/project_technical_report.md](reports/project_technical_report.md) – Formal technical documentation

### External References
- [Apache Airflow Documentation](https://airflow.apache.org/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [Pandas Documentation](https://pandas.pydata.org/)
- [JSONPlaceholder API](https://jsonplaceholder.typicode.com/)

---

## License & Attribution

This project is part of a comprehensive Data Engineering lab covering:
- ETL pipeline design
- Apache Airflow workflow orchestration
- PostgreSQL database management
- Docker containerization
- Cloud deployment (planned)

**Modules**: 1–15 completed, 16+ planned

---

## Contact & Support

For questions or issues:
1. Check [SETUP.md](SETUP.md) troubleshooting section
2. Review [reports/project_technical_report.md](reports/project_technical_report.md) for architecture details
3. Inspect logs in `logs/` directory
4. Check Airflow UI for task execution details

---

<!-- **Last Updated**: 2026-08-14 -->
**Status**: Module 15 ✅ Complete | Module 16 🚀 Ready for Dockerization
