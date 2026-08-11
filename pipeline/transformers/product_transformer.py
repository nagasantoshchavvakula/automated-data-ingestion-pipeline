import logging

import pandas as pd


logger = logging.getLogger(__name__)


# Business-approved product categories
ALLOWED_CATEGORIES = {
    "electronics",
    "kitchen",
    "clothing",
    "home",
    "books",
    "sports",
}


def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate product data before loading into PostgreSQL.
    """

    input_rows = len(df)
    
    logger.info("Starting product transformation | input_rows=%d", input_rows)

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
    # 2. Remove duplicate product IDs
    # ---------------------------------------------------------
    before = len(df)

    df = df.drop_duplicates(
        subset=["product_id"],
        keep="first"
    ).copy()

    duplicate_count = before - len(df)
    
    if duplicate_count:
        logger.info(
        "Product duplicates removed | count=%d",
        duplicate_count,
    )

    # ---------------------------------------------------------
    # 3. Clean product name
    # ---------------------------------------------------------
    if "name" in df.columns:
        df["name"] = (
            df["name"]
            .astype("string")
            .str.strip()
        )

    # ---------------------------------------------------------
    # 4. Normalize category
    # ---------------------------------------------------------
    if "category" in df.columns:
        df["category"] = (
            df["category"]
            .astype("string")
            .str.strip()
            .str.lower()
        )

        # Identify missing categories
        missing_category = df["category"].isna()

        missing_count = int(
            missing_category.sum()
        )

        if missing_count:
            logger.warning(
                "Products removed for missing category | count=%d",
                missing_count,
            )

            df = df[~missing_category].copy()

        # Identify categories outside approved list
        invalid_category = ~df["category"].isin(
            ALLOWED_CATEGORIES
        )

        invalid_count = int(
            invalid_category.sum()
        )

        if invalid_count:
            logger.warning(
                "Products removed for invalid category | count=%d",
                invalid_count,
            )

            df = df[~invalid_category].copy()
    # ---------------------------------------------------------
    # 5. Convert price to numeric
    # ---------------------------------------------------------
    if "price" in df.columns:
        df["price"] = pd.to_numeric(
            df["price"],
            errors="coerce"
        )

    # ---------------------------------------------------------
    # 6. Convert stock to numeric
    # ---------------------------------------------------------
    if "stock" in df.columns:
        df["stock"] = pd.to_numeric(
            df["stock"],
            errors="coerce"
        )

        # Missing stock becomes 0
        df["stock"] = df["stock"].fillna(0)

    # ---------------------------------------------------------
    # 7. Convert rating to numeric
    # ---------------------------------------------------------
    if "rating" in df.columns:
        df["rating"] = pd.to_numeric(
            df["rating"],
            errors="coerce"
        )

    # ---------------------------------------------------------
    # 8. Remove invalid prices
    # ---------------------------------------------------------
    if "price" in df.columns:
        before = len(df)

        df = df[
            df["price"] > 0
        ].copy()
        
        removed_count = before - len(df)
        
        if removed_count:
            logger.info(
                "Products removed for invalid price | count=%d",
                removed_count
            )

    # ---------------------------------------------------------
    # 9. Remove invalid ratings
    # ---------------------------------------------------------
    if "rating" in df.columns:
        before = len(df)

        df = df[
            df["rating"].isna()
            | df["rating"].between(0, 5)
        ].copy()

        removed_count = before - len(df)

        if removed_count:
            logger.info(
                "Products removed for invalid rating | count=%d",
                removed_count
            )

    # ---------------------------------------------------------
    # 10. Convert created_at to datetime
    # ---------------------------------------------------------
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(
            df["created_at"],
            errors="coerce",
            utc=True
        )

    # ---------------------------------------------------------
    # 11. Reset index
    # ---------------------------------------------------------
    df = df.reset_index(drop=True)

    logger.info(
        "Product transformation completed | input_rows=%d | output_rows=%d | removed_rows=%d",
        input_rows,
        len(df),
        input_rows - len(df)
    )

    return df