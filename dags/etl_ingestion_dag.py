from datetime import datetime, timedelta

from airflow.decorators import dag, task

from dag_utils import (
    run_customer_pipeline,
    run_product_pipeline,
    run_order_pipeline,
)


default_args = {
    "owner": "data_eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="etl_ingestion_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ingestion", "etl"],
)
def etl_ingestion_dag():

    # -----------------------------------------------------
    # Initialize Database
    # -----------------------------------------------------

    @task
    def initialize_db():

        from pipeline.loaders.db_connection import (
            init_db,
        )

        init_db()

        return True

    # -----------------------------------------------------
    # Customers
    # -----------------------------------------------------

    @task
    def process_customers(db_initialized):

        return run_customer_pipeline()

    # -----------------------------------------------------
    # Products
    # -----------------------------------------------------

    @task
    def process_products(db_initialized):

        return run_product_pipeline()

    # -----------------------------------------------------
    # Orders
    # -----------------------------------------------------

    @task
    def process_orders(db_initialized):

        return run_order_pipeline()

    # -----------------------------------------------------
    # Summary
    # -----------------------------------------------------

    @task
    def summarize(
        customer_result,
        product_result,
        order_result,
    ):

        print("=" * 60)
        print("AIRFLOW PIPELINE SUMMARY")
        print("=" * 60)

        print(
            f"Customers: {customer_result}"
        )

        print(
            f"Products:  {product_result}"
        )

        print(
            f"Orders:    {order_result}"
        )

        print("=" * 60)

    # -----------------------------------------------------
    # Task Dependencies
    # -----------------------------------------------------

    init_done = initialize_db()

    customer_result = process_customers(
        init_done
    )

    product_result = process_products(
        init_done
    )

    order_result = process_orders(
        init_done
    )

    summarize(
        customer_result,
        product_result,
        order_result,
    )


# Instantiate DAG

dag = etl_ingestion_dag()