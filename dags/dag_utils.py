import logging

from pipeline.loaders.db_connection import (
    SessionLocal,
)

from pipeline.extractors.csv_extractor import (
    extract_csv,
)

from pipeline.extractors.json_extractor import (
    extract_json,
)

from pipeline.extractors.api_extractor import (
    extract_api,
)

from pipeline.transformers.customer_transformer import (
    transform_customers,
)

from pipeline.transformers.product_transformer import (
    transform_products,
)

from pipeline.transformers.order_transformer import (
    transform_orders,
)

from pipeline.loaders.db_loader import (
    load_customers,
    load_products,
    load_orders,
)


logger = logging.getLogger(__name__)


def run_customer_pipeline():
    """
    Execute the customer ETL pipeline for an Airflow task.
    """

    logger.info("Starting Airflow customer pipeline")

    with SessionLocal() as session:

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
        result,
    )

    return result


def run_product_pipeline():
    """
    Execute the product ETL pipeline for an Airflow task.
    """

    logger.info("Starting Airflow product pipeline")

    with SessionLocal() as session:

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
        result,
    )

    return result


def run_order_pipeline():
    """
    Execute the order ETL pipeline for an Airflow task.
    """

    logger.info("Starting Airflow order pipeline")

    with SessionLocal() as session:

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
        result,
    )

    return result