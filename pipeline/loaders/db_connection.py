import logging
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
    )
from sqlalchemy.orm import sessionmaker, declarative_base

from pipeline.config import DATABASE_URL

logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# Database engine
# ---------------------------------------------------------
# Create SQLAlchemy engine
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

# ---------------------------------------------------------
# Session factory
# ---------------------------------------------------------
# Create session factory
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
)

# ---------------------------------------------------------
# Shared declarative base
# ---------------------------------------------------------
# Base class for ORM models
Base = declarative_base()

# ---------------------------------------------------------
# Customer model
# ---------------------------------------------------------
class Customer(Base):
    __tablename__ = "customers"

    customer_id = Column(Integer, primary_key=True)
    first_name = Column(String(50), nullable=False)
    last_name = Column(String(50), nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    phone = Column(String(20), nullable=True)
    country = Column(String(10), nullable=True)
    signup_date = Column(DateTime, nullable=True)
    annual_spend = Column(Float, nullable=True)

# ---------------------------------------------------------
# Product model
# ---------------------------------------------------------

class Product(Base):
    __tablename__ = "products"

    product_id = Column(
        Integer,
        primary_key=True,
    )

    name = Column(
        String(150),
        nullable=False,
    )

    category = Column(
        String(50),
        nullable=False,
    )

    price = Column(
        Float,
        nullable=False,
    )

    stock = Column(
        Integer,
        nullable=True,
    )

    rating = Column(
        Float,
        nullable=True,
    )

    created_at = Column(
        DateTime,
        nullable=True,
    )


# ---------------------------------------------------------
# Order model
# ---------------------------------------------------------
class Order(Base):
    __tablename__ = "orders"

    order_id = Column(
        Integer,
        primary_key=True,
    )

    customer_id = Column(
        Integer,
        ForeignKey("customers.customer_id"),
        nullable=False,
    )

    title = Column(
        String(255),
        nullable=False,
    )

    body = Column(
        Text,
        nullable=True,
    )
    
    quantity = Column(
        Integer,
        nullable=False,
        default=1,
    )

    order_date = Column(
        DateTime,
        nullable=False,
    )

# ---------------------------------------------------------
# Initialize database schema
# ---------------------------------------------------------
def init_db():
    """Create all database tables if they do not already exist."""

    Base.metadata.create_all(engine)
    
    logger.info("Database schema initialized successfully.")