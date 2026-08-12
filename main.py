import logging
import sys
import time

from pipeline.utils.logger import setup_logging

from pipeline.extractors.csv_extractor import extract_csv
from pipeline.extractors.json_extractor import extract_json
from pipeline.extractors.api_extractor import extract_api

from pipeline.transformers.customer_transformer import transform_customers
from pipeline.transformers.product_transformer import transform_products
from pipeline.transformers.order_transformer import transform_orders

from pipeline.loaders.db_connection import SessionLocal, init_db
from pipeline.loaders.db_loader import (
    load_customers,
    load_products,
    load_orders,
)


logger = logging.getLogger(__name__)


# =========================================================
# Customer Pipeline
# =========================================================

def run_customer_pipeline(session):
    """
    Execute the complete customer pipeline:

    Extract CSV
        ↓
    Transform customers
        ↓
    Load customers into PostgreSQL
    """

    logger.info("Starting customer pipeline")

    df_raw = extract_csv(
        "data/raw/customers.csv"
    )

    df_clean = transform_customers(
        df_raw
    )

    loaded_count = load_customers(
        session,
        df_clean
    )

    result = {
        "raw": len(df_raw),
        "clean": len(df_clean),
        "loaded": loaded_count,
    }

    logger.info(
        "Customer pipeline completed | %s",
        result
    )

    return result


# =========================================================
# Product Pipeline
# =========================================================

def run_product_pipeline(session):
    """
    Execute the complete product pipeline:

    Extract JSON
        ↓
    Transform products
        ↓
    Load products into PostgreSQL
    """

    logger.info("Starting product pipeline")

    df_raw = extract_json(
        "data/raw/products.json"
    )

    df_clean = transform_products(
        df_raw
    )

    loaded_count = load_products(
        session,
        df_clean
    )

    result = {
        "raw": len(df_raw),
        "clean": len(df_clean),
        "loaded": loaded_count,
    }

    logger.info(
        "Product pipeline completed | %s",
        result
    )

    return result


# =========================================================
# Order Pipeline
# =========================================================

def run_order_pipeline(session):
    """
    Execute the complete order pipeline:

    Extract REST API
        ↓
    Transform orders
        ↓
    Load orders into PostgreSQL
    """

    logger.info("Starting order pipeline")

    df_raw = extract_api(
        "https://jsonplaceholder.typicode.com/posts"
    )

    df_clean = transform_orders(
        df_raw
    )

    loaded_count = load_orders(
        session,
        df_clean
    )

    result = {
        "raw": len(df_raw),
        "clean": len(df_clean),
        "loaded": loaded_count,
    }

    logger.info(
        "Order pipeline completed | %s",
        result
    )

    return result


# =========================================================
# Main Pipeline
# =========================================================

def run_pipeline() -> int:
    """
    Execute all ETL pipelines and return an operating-system
    exit code.

    Returns:
        0 → complete success
        1 → one or more pipelines failed
    """

    start_time = time.perf_counter()

    # Configure logging first
    setup_logging()

    logger.info("=" * 70)
    logger.info("Starting Automated Data Ingestion Pipeline")
    logger.info("=" * 70)

    results = {}
    errors = {}

    try:
        # Ensure database tables exist
        logger.info("Initializing database schema")
        init_db()

        # Open one database session for this pipeline run
        with SessionLocal() as session:

            pipelines = [
                ("customers", run_customer_pipeline),
                ("products", run_product_pipeline),
                ("orders", run_order_pipeline),
            ]

            for name, runner in pipelines:

                try:
                    results[name] = runner(session)

                except Exception as exc:
                    logger.exception(
                        "%s pipeline failed",
                        name
                    )

                    errors[name] = str(exc)

    except Exception as exc:

        logger.exception(
            "Pipeline initialization failed"
        )

        errors["initialization"] = str(exc)

    elapsed_time = time.perf_counter() - start_time

    # =====================================================
    # Pipeline Summary
    # =====================================================

    logger.info("")
    logger.info("=" * 70)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 70)

    for name, result in results.items():

        logger.info(
            "%-10s | raw=%-5s | clean=%-5s | loaded=%-5s",
            name,
            result["raw"],
            result["clean"],
            result["loaded"],
        )

    if errors:

        logger.error("Errors:")

        for name, error in errors.items():

            logger.error(
                "%s → %s",
                name,
                error
            )

    else:

        logger.info("No pipeline errors")

    logger.info(
        "Elapsed time: %.2f seconds",
        elapsed_time
    )

    logger.info("=" * 70)

    # Return operating-system exit code
    return 0 if not errors else 1


# =========================================================
# Script Entry Point
# =========================================================

if __name__ == "__main__":
    sys.exit(run_pipeline())