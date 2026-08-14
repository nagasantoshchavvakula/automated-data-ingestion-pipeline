# Setup and Operations Guide

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Repository Setup](#repository-setup)
3. [Python Virtual Environment](#python-virtual-environment)
4. [Dependency Installation](#dependency-installation)
5. [Environment Configuration](#environment-configuration)
6. [PostgreSQL Setup](#postgresql-setup)
7. [Airflow Setup](#airflow-setup)
8. [Verification Steps](#verification-steps)
9. [Running the Pipeline](#running-the-pipeline)
10. [Local Monitoring](#local-monitoring)
11. [Troubleshooting](#troubleshooting)
12. [Module 16: Docker Preparation](#module-16-docker-preparation)

---

## Prerequisites

Before starting, ensure you have:

### System Requirements
- **OS**: Windows 10/11 with WSL2, or Linux/macOS
- **RAM**: 8GB minimum (16GB recommended)
- **Disk**: 5GB free space
- **Network**: Internet access for API calls

### Required Software

#### 1. Python 3.12+

**Check installation**:
```bash
python --version
python3 --version
```

**Windows**:
- Download from [python.org](https://www.python.org/downloads/)
- Or use Windows Package Manager: `winget install Python.Python.3.12`

**Linux (Ubuntu/Debian)**:
```bash
sudo apt update
sudo apt install python3.12 python3.12-venv python3-pip
```

**macOS**:
```bash
brew install python@3.12
```

#### 2. PostgreSQL 13+

**Windows**:
- Download from [postgresql.org](https://www.postgresql.org/download/windows/)
- Or use Windows Package Manager: `winget install PostgreSQL.PostgreSQL`
- Install with pgAdmin included

**Linux (Ubuntu/Debian)**:
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
sudo systemctl start postgresql
```

**macOS**:
```bash
brew install postgresql@15
brew services start postgresql@15
```

**Verify installation**:
```bash
psql --version
```

#### 3. Git

**Windows**:
- Download from [git-scm.com](https://git-scm.com/)
- Or: `winget install Git.Git`

**Linux**:
```bash
sudo apt install git
```

**macOS**:
```bash
brew install git
```

**Verify**:
```bash
git --version
```

#### 4. WSL2 (Windows Only)

If using Windows, WSL2 is optional but recommended for better PostgreSQL compatibility:

```powershell
# PowerShell as Administrator
wsl --install
wsl --set-default-version 2
```

Alternatively, use PostgreSQL Windows installer with TCP/IP enabled.

### Environment

- **Shell**: Windows PowerShell or bash (WSL2/Linux/macOS)
- **Text Editor**: VS Code, vim, nano, or IDE of choice
- **Terminal**: 1 or more terminal windows for running services

---

## Repository Setup

### 1. Clone Repository

```bash
git clone <repository_url>
cd 01_lab_data_ingestion_pipeline
```

Replace `<repository_url>` with your actual repository URL.

### 2. Verify Repository Structure

```bash
ls -la

# Windows PowerShell:
dir
```

Expected output includes:
```
airflow.cfg
airflow_venv/
dags/
data/
logs/
pipeline/
reports/
.env.example
.gitignore
README.md
SETUP.md
requirements.txt
```

### 3. Review .gitignore

**Important**: Sensitive files are already ignored:

```
# Environment
.env
.env.*

# Virtual environments
venv/
.venv/
airflow_venv/

# Airflow
airflow.db
standalone_admin_password.txt
logs/
```

**DO NOT commit**:
- `.env` (database passwords)
- `airflow.db` (user credentials)
- `standalone_admin_password.txt` (login password)
- `logs/` (may contain sensitive data)
- Virtual environments

---

## Python Virtual Environment

### Why Virtual Environments?

A virtual environment isolates project dependencies:
- No conflicts with system Python
- Easy dependency management
- Reproducible across machines
- Can have multiple environments with different versions

### 1. Create Virtual Environment

**Windows PowerShell**:
```powershell
python -m venv airflow_venv
```

**bash (WSL/Linux/macOS)**:
```bash
python3 -m venv airflow_venv
```

This creates the `airflow_venv/` directory with:
- `Lib/python3.12/site-packages/` – Installed packages
- `Scripts/` or `bin/` – Executable scripts
- `pyvenv.cfg` – Configuration

### 2. Activate Virtual Environment

**Windows PowerShell**:
```powershell
.\airflow_venv\Scripts\Activate.ps1
```

**Windows Command Prompt (cmd.exe)**:
```cmd
airflow_venv\Scripts\activate.bat
```

**bash (WSL/Linux/macOS)**:
```bash
source airflow_venv/bin/activate
```

**Verification** (any shell):
```bash
which python
# or
where python

# Output should show airflow_venv in path
```

### 3. Verify Python Version

```bash
python --version
# Output: Python 3.12.x
```

### 4. Deactivate Environment (When Needed)

```bash
deactivate
```

---

## Dependency Installation

### 1. Upgrade pip

**Important**: Always upgrade pip before installing packages.

```bash
python -m pip install --upgrade pip

# Verify
pip --version
# Output: pip 24.x.x from C:\...\airflow_venv\...
```

### 2. Install Requirements

With virtual environment activated:

```bash
pip install -r requirements.txt
```

This installs:
- Apache Airflow 2.10.2
- PostgreSQL provider (airflow-providers-postgres)
- SQLAlchemy & psycopg2 (database drivers)
- Pandas (data manipulation)
- Requests (HTTP client)
- python-dotenv (environment variables)
- All dependencies

### 3. Verify Installation

```bash
# Check Airflow
airflow version
# Output: 2.10.2

# Check Python packages
pip list | grep -i airflow
pip list | grep -i pandas
pip list | grep -i sqlalchemy
```

### Installation Troubleshooting

**Issue**: `ERROR: Could not find a version that satisfies the requirement`
```bash
# Solution: Upgrade pip
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

**Issue**: `Permission denied: ...bin/airflow`
```bash
# Solution: Activate virtual environment
source airflow_venv/bin/activate  # bash/WSL
.\airflow_venv\Scripts\Activate.ps1  # PowerShell
```

**Issue**: `ModuleNotFoundError: No module named 'airflow'`
```bash
# Solution: Check if virtual environment is activated
which python  # Should show airflow_venv in path
pip install -r requirements.txt
```

---

## Environment Configuration

### 1. Create .env File

```bash
cp .env.example .env
```

Or manually create `c:\Users\91900\Emonics_Projects_LabWork\01_lab_data_ingestion_pipeline\.env`:

### 2. Edit .env with Your Credentials

**DO NOT use the exact values below** – replace with your actual PostgreSQL credentials.

```bash
# PostgreSQL - Pipeline Database
DB_USER=pipeline_user
DB_PASS=<your_strong_password>
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ingestion_db

# PostgreSQL - Airflow Metadata Database
AIRFLOW_DB_USER=airflow_user
AIRFLOW_DB_PASS=<your_strong_password>
AIRFLOW_DB_HOST=localhost
AIRFLOW_DB_PORT=5432
AIRFLOW_DB_NAME=airflow_db

# Database URLs (for SQLAlchemy)
DATABASE_URL=postgresql://pipeline_user:<your_strong_password>@localhost:5432/ingestion_db
AIRFLOW_DB_URL=postgresql://airflow_user:<your_strong_password>@localhost:5432/airflow_db

# External API
API_BASE_URL=https://jsonplaceholder.typicode.com
API_TIMEOUT=10

# Logging
LOG_LEVEL=INFO
```

### 3. Secure .env File

**IMPORTANT**: Protect your credentials!

**Windows**:
```powershell
# Restrict to current user only
icacls .env /inheritance:r /grant:r "$env:USERNAME`:F"
```

**Linux/macOS**:
```bash
chmod 600 .env
```

### 4. Verify .env is Loaded

```bash
python -c "from dotenv import load_dotenv; from pipeline.config import DB_USER; load_dotenv(); import os; print(f'DB_USER={os.getenv(\"DB_USER\")}')"
```

Should output: `DB_USER=pipeline_user` (or your configured value)

---

## PostgreSQL Setup

### 1. Start PostgreSQL Service

**Windows (installed as service)**:
```powershell
# Already running if installed
Get-Service PostgreSQL*
```

**Linux**:
```bash
sudo systemctl start postgresql
sudo systemctl enable postgresql  # Start on boot
sudo systemctl status postgresql
```

**macOS**:
```bash
brew services start postgresql@15
```

### 2. Verify PostgreSQL is Running

**Any platform**:
```bash
psql --version
```

Should output PostgreSQL version (e.g., `psql (PostgreSQL) 15.x`)

### 3. Create Pipeline Database User

Open `psql` (PostgreSQL interactive terminal):

**Windows (start > run > "Services")**: Right-click PostgreSQL → Start
Then open Command Prompt and type:
```bash
psql -U postgres
```

**Linux/macOS**:
```bash
sudo -u postgres psql
```

### 4. Execute Database Setup Commands

Once in `psql` prompt (you'll see `postgres=#`):

```sql
-- Create Airflow metadata database and user
CREATE USER airflow_user WITH PASSWORD '<your_strong_password>';
CREATE DATABASE airflow_db OWNER airflow_user;

-- Create pipeline application database and user
CREATE USER pipeline_user WITH PASSWORD '<your_strong_password>';
CREATE DATABASE ingestion_db OWNER pipeline_user;

-- Grant all privileges
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
GRANT ALL PRIVILEGES ON DATABASE ingestion_db TO pipeline_user;

-- Verify creation
\l  -- List databases
\du -- List users

-- Exit
\q
```

### 5. Verify Database Connection

```bash
# Test pipeline database connection
psql -U pipeline_user -h localhost -d ingestion_db -c "SELECT version();"

# Test Airflow database connection
psql -U airflow_user -h localhost -d airflow_db -c "SELECT version();"
```

Both should output PostgreSQL version information without errors.

### Database Troubleshooting

**Issue**: `FATAL: Ident authentication failed for user "pipeline_user"`

**Solution** (Linux): Edit `/etc/postgresql/*/main/pg_hba.conf`, change `ident` to `md5`, then restart PostgreSQL:
```bash
sudo systemctl restart postgresql
```

**Issue**: `FATAL: role "pipeline_user" does not exist`

**Solution**: Run the database setup commands above in `psql`

**Issue**: `could not connect to server: Connection refused`

**Solution**: 
- Windows: Check PostgreSQL service is running
- Linux: `sudo systemctl start postgresql`
- macOS: `brew services start postgresql@15`

---

## Airflow Setup

### 1. Set Airflow Home Directory

**Windows PowerShell**:
```powershell
$env:AIRFLOW_HOME = "$PWD"
[System.Environment]::SetEnvironmentVariable("AIRFLOW_HOME", $PWD, "User")
```

**bash**:
```bash
export AIRFLOW_HOME=$(pwd)
echo "export AIRFLOW_HOME=$(pwd)" >> ~/.bashrc
source ~/.bashrc
```

**Verify**:
```bash
echo $AIRFLOW_HOME
# or
echo %AIRFLOW_HOME%
```

### 2. Initialize Airflow Database

This creates the SQLite/PostgreSQL metadata database:

```bash
airflow db migrate
```

Output should include:
```
[201X-XX-XX XX:XX:XX,XXX] ...done
```

### 3. Create Airflow Admin User

Create your login credentials for Airflow UI:

```bash
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

**Save these credentials** – you'll use them to login to Airflow UI.

**Verify user creation**:
```bash
airflow users list
```

### 4. Verify DAGS_FOLDER Configuration

Edit `airflow.cfg` and verify:

```ini
[core]
dags_folder = /full/path/to/dags

# Example on Windows:
# dags_folder = C:\Users\91900\Emonics_Projects_LabWork\01_lab_data_ingestion_pipeline\dags

# Example on WSL/Linux:
# dags_folder = /home/user/01_lab_data_ingestion_pipeline/dags
```

**Important**: Use absolute paths (not relative paths like `./dags`)

### 5. Verify Airflow Installation

```bash
airflow version
# Output: 2.10.2

airflow info
# Shows detailed Airflow configuration

airflow config get-value core dags_folder
# Shows configured DAGS_FOLDER path
```

---

## Verification Steps

### 1. Verify Python Package Imports

Test that pipeline module can be imported:

**Windows PowerShell**:
```powershell
python -c "from pipeline.config import DB_USER; print('✓ Pipeline module imports successfully')"
```

**bash**:
```bash
python -c "from pipeline.config import DB_USER; print('✓ Pipeline module imports successfully')"
```

**Expected output**:
```
✓ Pipeline module imports successfully
```

**Troubleshooting if import fails**:
```bash
# Add project root to PYTHONPATH
# Windows PowerShell:
$env:PYTHONPATH = "$PWD;$env:PYTHONPATH"

# bash:
export PYTHONPATH="$(pwd):$PYTHONPATH"

# Then try import again
python -c "from pipeline.config import DB_USER"
```

### 2. Verify Airflow Installation

```bash
airflow version
# Output: 2.10.2

airflow dags list
# Should show: etl_ingestion_pipeline (if DAG is valid)
```

### 3. Check DAG for Import Errors

```bash
airflow dags list -d
# Shows DAGs and any import errors
```

If errors appear, check:
- Is `dags/` in PYTHONPATH?
- Do all imports in `dags/etl_ingestion_dag.py` exist?
- Is `dag_utils.py` present?

**Test DAG explicitly**:
```bash
python -c "from dags.etl_ingestion_dag import dag; print(f'DAG loaded: {dag.dag_id}')"
```

### 4. List DAG Tasks

```bash
airflow tasks list etl_ingestion_pipeline
```

Expected output:
```
initialize_db
process_customers
process_products
process_orders
summarize
```

If tasks don't appear:
- Check `dags/etl_ingestion_dag.py` syntax
- Verify task function names match
- Check for Python indentation errors

### 5. Verify PostgreSQL Connectivity

Test database connections:

```python
# Create test_connection.py
from pipeline.config import DATABASE_URL
from sqlalchemy import create_engine, text

try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("✓ Database connection successful")
except Exception as e:
    print(f"✗ Database connection failed: {e}")
```

Then run:
```bash
python test_connection.py
```

### 6. Check Airflow Configuration

Verify key settings:

```bash
airflow config get-value core dags_folder
airflow config get-value core executor
airflow config get-value core default_timezone
```

---

## Running the Pipeline

### Option 1: Airflow Standalone Mode (Recommended for Local Development)

Standalone mode runs everything in one process (scheduler, webserver, etc):

**Start Airflow**:
```bash
airflow standalone
```

**Output** (watch for):
```
standalone | Starting Airflow Standalone
standalone | Airflow UI available at http://localhost:8080
standalone | Use the following credentials to login:
standalone |     username: admin
standalone |     password: <random_password>
```

**In another terminal**, open Airflow UI:
- **URL**: http://localhost:8080
- **Login**: Use credentials from setup or output above

### Option 2: Traditional Mode (Scheduler + Webserver)

In two separate terminals:

**Terminal 1 - Scheduler**:
```bash
airflow scheduler
```

**Terminal 2 - Webserver**:
```bash
airflow webserver --port 8080
```

Then open http://localhost:8080

### Option 3: Command-Line Trigger (No UI)

If you just want to run the DAG without the UI:

```bash
# Trigger a single DAG run
airflow dags trigger etl_ingestion_pipeline

# Monitor execution
airflow dags list-runs -d etl_ingestion_pipeline --state running

# Check final status
airflow dags list-runs -d etl_ingestion_pipeline
```

---

## Local Monitoring

### 1. Airflow Web UI

**Navigate to**: http://localhost:8080
**Login**: Username and password from setup

**Key pages**:
- **DAGs**: List all DAGs, toggle pause/unpause
- **Graph View**: Visual dependency graph of tasks
- **Tree View**: Timeline view of DAG runs
- **Admin → Logs**: View task execution logs

### 2. View Task Logs

**Via CLI**:
```bash
# List all logs
ls logs/dag_id=etl_ingestion_pipeline/

# View specific task log
airflow tasks logs etl_ingestion_pipeline initialize_db 2026-08-14

# Follow log in real-time (Linux/macOS)
tail -f logs/pipeline.log
```

**Via Airflow UI**:
- Graph View → Click task → View Logs

### 3. Check Database Content

After a successful run:

```bash
# Connect to database
psql -U pipeline_user -h localhost -d ingestion_db

# List tables
\dt

# Count rows in each table
SELECT COUNT(*) FROM customers;
SELECT COUNT(*) FROM products;
SELECT COUNT(*) FROM orders;

# Sample data
SELECT * FROM customers LIMIT 5;
SELECT * FROM products LIMIT 5;
SELECT * FROM orders LIMIT 5;
```

### 4. Application Logs

The pipeline also writes logs to file:

```bash
# View application logs
cat logs/pipeline.log

# Follow logs in real-time (Linux/macOS)
tail -f logs/pipeline.log

# Search for errors
grep -i error logs/pipeline.log
grep -i warning logs/pipeline.log
```

### 5. Monitor Airflow Database

View Airflow metadata:

```bash
psql -U airflow_user -h localhost -d airflow_db

# List DAG runs
SELECT * FROM dag_run LIMIT 5;

# List task instances
SELECT * FROM task_instance WHERE dag_id = 'etl_ingestion_pipeline' LIMIT 5;
```

---

## Troubleshooting

### DAG and Airflow Issues

| Symptom | Cause | How to Check | How to Fix |
|---------|-------|-------------|-----------|
| "DAG not appearing in Airflow" | DAG file not in dags_folder | `airflow config get-value core dags_folder` | Update AIRFLOW_HOME or airflow.cfg |
| | DAG file contains errors | `python -c "from dags.etl_ingestion_dag import dag"` | Fix Python syntax errors |
| | dags folder not configured | `airflow dags list` shows no DAGs | Edit airflow.cfg with correct path |
| "Import error: cannot find module pipeline" | PYTHONPATH not set | `echo $PYTHONPATH` (bash) or `echo %PYTHONPATH%` (Windows) | Add project root to PYTHONPATH |
| | Virtual environment not activated | `which python` (should show airflow_venv) | Activate virtual environment |
| "Airflow tasks not appearing" | DAG definition incorrect | `airflow tasks list etl_ingestion_pipeline` | Check @task decorators in etl_ingestion_dag.py |
| | Module imports failed | Check Airflow UI → Admin → Logs | Fix import statements |
| "Airflow login incorrect" | Wrong password | Verify credentials | Reset: `airflow users delete admin` then `airflow users create ...` |
| | Multiple users created | `airflow users list` | Use correct username |

### Database Issues

| Symptom | Cause | How to Check | How to Fix |
|---------|-------|-------------|-----------|
| "psycopg2.OperationalError: connection refused" | PostgreSQL not running | `psql --version` | Start PostgreSQL service |
| | Wrong host/port | Check .env DB_HOST, DB_PORT | Update .env with correct values |
| | Database doesn't exist | `psql -U pipeline_user -d ingestion_db -c "SELECT 1"` | Run database setup commands |
| "FATAL: Ident authentication failed" | Linux pg_hba.conf not configured | `/etc/postgresql/*/main/pg_hba.conf` | Change `ident` to `md5`, restart PostgreSQL |
| "role pipeline_user does not exist" | User not created | `psql -U postgres -c "\du"` | Create user in database setup |
| "permission denied for schema public" | User lacks privileges | `psql -U postgres -d ingestion_db -c "\dp schema public"` | Grant privileges in database setup |

### Pipeline Execution Issues

| Symptom | Cause | How to Check | How to Fix |
|---------|-------|-------------|-----------|
| "Task failed: FileNotFoundError" | Data file missing | `ls data/raw/` | Copy data files to correct location |
| "Task failed: email validation" | Invalid email in CSV | Check logs, sample data | Fix data in source CSV file |
| "UPSERT failed: unique constraint" | Duplicate unique key (email) | Check logs for detail | Data has duplicate unique keys, review transformer |
| "API extraction failed: timeout" | API too slow or unreachable | `curl https://jsonplaceholder.typicode.com/posts` | Check internet connection or API status |
| "Task retried and succeeded" | Transient network error | Check logs | Normal behavior, check if consistent |
| "All tasks show no status" | Airflow database corrupted | `airflow db check` | Reset: `airflow db reset` (CAUTION: deletes all metadata) |

### Environment & Configuration Issues

| Symptom | Cause | How to Check | How to Fix |
|---------|-------|-------------|-----------|
| ".env file not loaded" | python-dotenv not installed | `pip list \| grep dotenv` | `pip install python-dotenv` |
| | .env in wrong location | `ls .env` (should be in project root) | Move .env to project root |
| "DB_PASS is not set in the .env file" | Missing password variable | `grep DB_PASS .env` | Add DB_PASS to .env |
| "API_BASE_URL not configured" | Variable not in .env | `grep API_BASE_URL .env` | Add API_BASE_URL to .env |
| "Airflow UI not accessible" | Port 8080 in use | `netstat -an \| grep 8080` (Windows) | Change port: `airflow webserver --port 8081` |
| | Webserver not started | `curl http://localhost:8080` | Start webserver: `airflow webserver --port 8080` |

### Windows-Specific Issues

| Symptom | Cause | How to Check | How to Fix |
|---------|-------|-------------|-----------|
| "Script execution is disabled" | PowerShell execution policy | `Get-ExecutionPolicy` | `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned` |
| "Permission denied" on .env | File permissions | Run as Administrator | `icacls .env /inheritance:r /grant:r "$env:USERNAME`:F"` |
| "Module not found" | Path separator issues | Check path in error | Use forward slashes in paths or `pathlib.Path` |
| "PostgreSQL not starting" | Service misconfiguration | Services (services.msc) | Reinstall PostgreSQL with proper user privileges |

### WSL2-Specific Issues

| Symptom | Cause | How to Check | How to Fix |
|---------|-------|-------------|-----------|
| "Cannot connect to PostgreSQL on localhost" | PostgreSQL runs on Windows, not WSL | `wsl ps aux \| grep postgres` | Connect to Windows IP: `DB_HOST=172.17.0.1` or install PostgreSQL in WSL |
| "File permissions issues" | WSL filesystem permissions | `ls -la data/raw/` | Change permissions: `chmod 755 data/` |
| "Python/pip not found" | Python not installed in WSL | `wsl which python3` | Install: `sudo apt install python3.12 python3.12-venv` |
| "/mnt/c path issues" | Windows drive mount permissions | Create symlink in WSL home | `ln -s /mnt/c/Users/.../project ~/project` |

### Recovery Steps

If something breaks severely:

**Option 1: Reset Airflow Metadata** (CAUTION: deletes all runs/logs)
```bash
airflow db reset
airflow db migrate
airflow users create --username admin --password admin --role Admin
```

**Option 2: Clean Virtual Environment**
```bash
deactivate
rm -rf airflow_venv
python -m venv airflow_venv
# Activate and reinstall
pip install -r requirements.txt
```

**Option 3: Rebuild Databases**
```sql
-- In psql as postgres user
DROP DATABASE airflow_db;
DROP DATABASE ingestion_db;
DROP USER airflow_user;
DROP USER pipeline_user;

-- Then re-run database setup
```

**Option 4: Full Reset (Nuclear Option)**
```bash
# Backup everything first!
cp -r dags/ dags.backup
cp -r data/ data.backup
cp -r logs/ logs.backup

# Then clean and restart
rm -rf airflow_venv airflow.db
rm -rf logs/*
rm -rf pipeline/__pycache__ dags/__pycache__

# Recreate and reinstall
python -m venv airflow_venv
# Activate and reinstall all
```

---

## Module 16: Docker Preparation

### Current State (Module 15)

The pipeline runs locally with:
- Python virtual environment
- Local PostgreSQL databases
- Airflow scheduler + webserver on localhost:8080
- Direct file access to dags/, pipeline/, data/

### Module 16 Objective

Containerize the pipeline using Docker:
- Airflow runs inside Docker container
- PostgreSQL runs inside Docker container
- Docker Compose orchestrates both services
- Volumes mount source code and data

### Why Docker?

| Aspect | Local | Docker |
|--------|-------|--------|
| **Environment** | Depends on OS, installed software | Identical across all machines |
| **Scalability** | Single machine | Horizontally scalable |
| **Deployment** | Manual setup on each server | Automatic via docker-compose |
| **Isolation** | Conflicts with other projects | Isolated containers |
| **Development** | Different configs locally vs prod | Same config everywhere |

### Key Docker Concepts

**Image**: A blueprint/template for containers
- Think: Python class definition
- Created from `Dockerfile`
- Example: `apache/airflow:2.10.2-python3.12`

**Container**: A running instance of an image
- Think: Object instance of a class
- Isolated filesystem, processes, environment
- Can be started, stopped, restarted

**Docker Compose**: Orchestrates multiple containers
- Define services in `docker-compose.yml`
- Each service is a container
- Services communicate by name over network
- Manage volumes, environment, ports

**Volume**: Persistent storage
- **Bind mount**: Host directory appears inside container
  - Example: `./dags:/usr/local/airflow/dags` (read-only source code)
- **Named volume**: Docker-managed storage
  - Example: `postgres_data:/var/lib/postgresql/data` (persistent database)

**Network**: Communication between containers
- Services can reference each other by name
- Airflow container → postgresql service (not localhost)

### Critical: Connection Strings in Docker

**Local execution** (Module 15):
```python
DATABASE_URL = "postgresql://user:pass@localhost:5432/ingestion_db"
#                                     ^^^^^^^ - connects to local machine
```

**Docker execution** (Module 16):
```python
DATABASE_URL = "postgresql://user:pass@postgres:5432/ingestion_db"
#                                      ^^^^^^^ - service name in docker-compose
```

The Airflow container cannot use "localhost" because localhost refers to itself (the container), not the host machine. Instead, it uses the Docker Compose service name `postgres`.

### Planned Docker Architecture (Not Yet Implemented)

```
┌────────────────────────────────────────────────────────┐
│           Docker Compose Network                       │
│                                                        │
│  ┌──────────────────────┐  ┌────────────────────────┐│
│  │  airflow service     │  │ postgres service       ││
│  │                      │  │                        ││
│  │  - Scheduler         │  │ - PostgreSQL 15        ││
│  │  - Webserver:8080    │→→│ - Port: 5432           ││
│  │  - Python 3.12       │  │ - airflow_db           ││
│  │  - Airflow 2.10.2    │  │ - ingestion_db         ││
│  │                      │  │                        ││
│  │  Volumes:            │  │  Volumes:              ││
│  │  - ./dags            │  │  - postgres_data       ││
│  │  - ./pipeline        │  │  - postgres_logs       ││
│  │  - ./logs            │  │                        ││
│  │  - ./data            │  │                        ││
│  └──────────────────────┘  └────────────────────────┘│
│                                                        │
└────────────────────────────────────────────────────────┘
```

### What Will Change in Module 16

**Files to create**:
1. `Dockerfile` – Image definition for Airflow
2. `docker-compose.yml` – Multi-container orchestration
3. `.dockerignore` – Files excluded from build context
4. `docker/.env` – Docker-specific environment variables

**Files to modify**:
1. `pipeline/config.py` – Detect Docker mode, adjust DATABASE_URL
2. `SETUP.md` – Add Docker setup instructions (separate section)

**Files to NOT modify**:
1. Source code in `dags/` and `pipeline/`
2. Data extraction/transformation logic
3. Database schema
4. Airflow DAG definition

### Preparation Checklist for Module 16

- [x] Complete Module 15 (local pipeline works)
- [x] All 5 tasks execute successfully
- [x] Documentation complete (this file)
- [ ] Docker Desktop installed (not yet required)
- [ ] Docker Compose available (not yet required)
- [ ] Dockerfile created (Module 16 task)
- [ ] docker-compose.yml created (Module 16 task)
- [ ] Docker networking tested (Module 16 task)
- [ ] Dockerized pipeline tested (Module 16 task)

### Getting Started with Docker (When Ready)

When Module 16 begins:

1. **Install Docker Desktop**
   - Windows: https://www.docker.com/products/docker-desktop
   - macOS: https://www.docker.com/products/docker-desktop
   - Linux: `sudo apt install docker.io docker-compose`

2. **Understand Docker concepts** (5-10 minutes reading)
   - Images, containers, volumes, networks
   - Dockerfile syntax
   - docker-compose.yml structure

3. **Create Dockerfile for Airflow**
   - Base image: apache/airflow:2.10.2-python3.12
   - Install additional packages if needed
   - Set environment variables

4. **Create docker-compose.yml**
   - Service: airflow (Airflow container)
   - Service: postgres (PostgreSQL container)
   - Define volumes, ports, environment, networks

5. **Build and test locally**
   - `docker-compose build`
   - `docker-compose up`
   - Verify Airflow UI on http://localhost:8080
   - Verify database connectivity

6. **Run DAG inside Docker**
   - Trigger DAG from Airflow UI
   - Verify all tasks succeed
   - Check database for loaded data

### Docker Resources

- [Docker Documentation](https://docs.docker.com/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Apache Airflow in Docker](https://airflow.apache.org/docs/docker-compose/)
- [PostgreSQL Docker Hub](https://hub.docker.com/_/postgres)

---

## Summary Checklist

### Before Running Pipeline

- [ ] Python 3.12+ installed
- [ ] PostgreSQL 13+ running
- [ ] Repository cloned
- [ ] Virtual environment created and activated
- [ ] Dependencies installed (`pip install -r requirements.txt`)
- [ ] `.env` file created with correct credentials
- [ ] PostgreSQL databases created (`airflow_db`, `ingestion_db`)
- [ ] PostgreSQL users created (`airflow_user`, `pipeline_user`)
- [ ] Airflow initialized (`airflow db migrate`)
- [ ] Airflow admin user created (`airflow users create ...`)
- [ ] DAG imports successfully (`airflow dags list`)
- [ ] Database connection works (`psql -U pipeline_user -d ingestion_db`)

### When Starting Pipeline Execution

- [ ] Airflow started (`airflow standalone` or `airflow scheduler` + `airflow webserver`)
- [ ] Airflow UI accessible (http://localhost:8080)
- [ ] DAG visible in UI (`etl_ingestion_pipeline`)
- [ ] DAG is unpaused (toggle switch ON)
- [ ] Trigger DAG manually (UI button or CLI)
- [ ] Monitor execution in Graph View
- [ ] Check logs for errors
- [ ] Verify database content after run

### Post-Execution Validation

- [ ] All 5 tasks show SUCCESS status
- [ ] No retries needed (0 retries)
- [ ] `summarize` task shows all data counts
- [ ] PostgreSQL tables have expected row counts
- [ ] No errors in logs
- [ ] Re-run DAG to verify idempotency

---

## Contact & Support

For issues:
1. Check this **Troubleshooting** section
2. Review [README.md](README.md) for architecture details
3. Check Airflow logs: `logs/` directory
4. Check application logs: `logs/pipeline.log`
5. Inspect Airflow database: `airflow db shell`

---

