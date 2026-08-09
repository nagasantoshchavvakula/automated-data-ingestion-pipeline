from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base

from pipeline.config import DATABASE_URL


# Create SQLAlchemy engine
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)


# Create session factory
SessionLocal = sessionmaker(
    bind=engine
)


# Base class for ORM models
Base = declarative_base()


class Customer(Base):
    __tablename__ = "customers"

    customer_id = Column(Integer, primary_key=True)
    first_name = Column(String(50), nullable=False)
    last_name = Column(String(50))
    email = Column(String(100), unique=True, nullable=False)
    phone = Column(String(20))
    country = Column(String(10))
    signup_date = Column(DateTime)
    annual_spend = Column(Float)


def init_db():
    """Create all database tables."""
    Base.metadata.create_all(engine)