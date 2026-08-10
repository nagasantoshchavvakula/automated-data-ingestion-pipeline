import logging

import pandas as pd
from sqlalchemy.dialects.postgresql import insert

from pipeline.loaders.db_connection import (
    Customer,
    Product,
    Order,
)


logger = logging.getLogger(__name__)


def _prepare_records(df: pd.DataFrame) -> list[dict]:
    """
    Convert a Pandas DataFrame into database-safe Python dictionaries.

    Pandas values such as NaN, NaT, pd.NA, and Timestamp
    can cause problems with database drivers, so they are
    converted into native Python values.
    """

    if df.empty:
        return []

    safe_df = df.astype(object).where(pd.notnull(df), None)

    records = safe_df.to_dict(orient="records")

    return records


def _upsert(
    session,
    model,
    records: list[dict],
    conflict_column: str,
) -> int:
    """
    Generic PostgreSQL UPSERT helper.

    If the conflict column already exists, update the existing
    record. Otherwise, insert a new record.

    Returns:
        Number of records processed.
    """

    if not records:
        return 0

    table = model.__table__

    # Build INSERT statement
    stmt = insert(table).values(records)

    # Identify primary-key columns
    pk_columns = {
        column.name
        for column in table.primary_key.columns
    }

    # Update every non-primary-key column
    update_dict = {
        column.name: getattr(stmt.excluded, column.name)
        for column in table.columns
        if column.name not in pk_columns
    }

    # PostgreSQL ON CONFLICT DO UPDATE
    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=[conflict_column],
        set_=update_dict,
    )

    try:
        session.execute(upsert_stmt)
        session.commit()

        logger.info(
            "UPSERT completed | table=%s | rows=%d",
            table.name,
            len(records),
        )

        return len(records)

    except Exception:
        session.rollback()

        logger.exception(
            "UPSERT failed | table=%s",
            table.name,
        )

        raise


def load_customers(session, df: pd.DataFrame) -> int:
    """
    Load customer records into PostgreSQL.

    Uses customer_id as the conflict key.
    """

    records = _prepare_records(df)

    if not records:
        logger.info("No customer records to load")
        return 0

    logger.info(
        "Loading customers | rows=%d",
        len(records),
    )

    return _upsert(
        session=session,
        model=Customer,
        records=records,
        conflict_column="customer_id",
    )


def load_products(session, df: pd.DataFrame) -> int:
    """
    Load product records into PostgreSQL.

    Uses product_id as the conflict key.
    """

    records = _prepare_records(df)

    if not records:
        logger.info("No product records to load")
        return 0

    logger.info(
        "Loading products | rows=%d",
        len(records),
    )

    return _upsert(
        session=session,
        model=Product,
        records=records,
        conflict_column="product_id",
    )


def load_orders(session, df: pd.DataFrame) -> int:
    """
    Load order records into PostgreSQL.

    Uses order_id as the conflict key.
    """

    records = _prepare_records(df)

    if not records:
        logger.info("No order records to load")
        return 0

    logger.info(
        "Loading orders | rows=%d",
        len(records),
    )

    return _upsert(
        session=session,
        model=Order,
        records=records,
        conflict_column="order_id",
    )