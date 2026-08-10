import os
from dotenv import load_dotenv
from sqlalchemy.engine import URL

# Load environment variables from .env file
load_dotenv()

DB_USER = os.getenv("DB_USER", "pipeline_user")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "ingestion_db")

# Validate required password
if not DB_PASS:
    raise ValueError("DB_PASS is not set in the .env file")

# Build the PostgreSQL connection URL safely.
# URL.create() handles special characters in passwords.
DATABASE_URL = URL.create(
    drivername="postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=int(DB_PORT),
    database=DB_NAME
)

# External API configuration
API_BASE_URL = os.getenv(
    "API_BASE_URL",
    "https://jsonplaceholder.typicode.com"
)