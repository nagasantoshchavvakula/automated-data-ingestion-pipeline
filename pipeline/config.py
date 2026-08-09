import os
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("DB_USER", "pipeline_user")
DB_PASS = os.getenv("DB_PASS", "secret")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "ingestion_db")

DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASS}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)