import logging

import pandas as pd


logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# Synthetic mapping for JSONPlaceholder userId
# ---------------------------------------------------------
# JSONPlaceholder provides userId values from 1-10.
# Our customers table uses different business IDs.
#
# This mapping is intentionally deterministic because
# JSONPlaceholder is only being used as a mock orders API
# for this lab.
CUSTOMER_ID_MAP = {
    1: 101,
    2: 102,
    3: 104,
    4: 105,
    5: 107,
    6: 999,
    7: 101,
    8: 102,
    9: 104,
    10: 105,
}

def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform JSONPlaceholder API records into the
    relational order structure used by the pipeline.
    """

    logger.info("Starting order transformation | input_rows=%d", len(df))

    df = df.copy()

    # ---------------------------------------------------------
    # 1. Standardize column names
    # ---------------------------------------------------------
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )

    # ---------------------------------------------------------
    # 2. Rename API fields
    # ---------------------------------------------------------
    rename_map = {
        "id": "order_id",
        "userid": "customer_id",
    }

    df = df.rename(columns=rename_map)

    # ---------------------------------------------------------
    # 3. Keep only required API fields
    # ---------------------------------------------------------
    required_source_columns = [
        "order_id",
        "customer_id",
        "title",
        "body",
    ]

    existing_columns = [
        column
        for column in required_source_columns
        if column in df.columns
    ]

    df = df[existing_columns].copy()

    # ---------------------------------------------------------
    # 4. Convert IDs to numeric
    # ---------------------------------------------------------
    if "order_id" in df.columns:
        df["order_id"] = pd.to_numeric(
            df["order_id"],
            errors="coerce"
        )

    if "customer_id" in df.columns:
        df["customer_id"] = pd.to_numeric(
            df["customer_id"],
            errors="coerce"
        )
    
    # ---------------------------------------------------------
    # 5. Map API userId to database customer_id
    # ---------------------------------------------------------

    df["customer_id"] = df["customer_id"].map(
        CUSTOMER_ID_MAP
    )
    
    # ---------------------------------------------------------
    # 5. Clean title and body
    # ---------------------------------------------------------
    if "title" in df.columns:
        df["title"] = (
            df["title"]
            .astype("string")
            .str.strip()
        )

    if "body" in df.columns:
        df["body"] = (
            df["body"]
            .astype("string")
            .str.strip()
        )

    # ---------------------------------------------------------
    # 6. Remove records missing foreign keys
    # ---------------------------------------------------------
    before = len(df)

    df = df.dropna(
        subset=["order_id", "customer_id"]
    ).copy()
    
    removed_count = before - len(df)

    logger.info(
        "Orders removed for missing IDs | count=%d",
        removed_count
    )
    
    # ---------------------------------------------------------
    # 9. Convert order_id to integer
    # ---------------------------------------------------------

    df["order_id"] = df["order_id"].astype(int)

    df["customer_id"] = df["customer_id"].astype(int)

    # ---------------------------------------------------------
    # 10. Create synthetic quantity
    # ---------------------------------------------------------

    # JSONPlaceholder does not provide quantity.
    # We use 1 as the controlled lab default.

    df["quantity"] = 1

    # ---------------------------------------------------------
    # 8. Create synthetic order date
    # ---------------------------------------------------------
    # JSONPlaceholder does not provide an order timestamp.
    # Use the current UTC timestamp for this lab.
    df["order_date"] = pd.Timestamp.now(tz="UTC")

    # ---------------------------------------------------------
    # 9. Reset index
    # ---------------------------------------------------------
    df = df.reset_index(drop=True)

    logger.info(
        "Order transformation completed | output_rows=%d",
        len(df)
    )

    return df