import logging

import pandas as pd


logger = logging.getLogger(__name__)


def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize customer data before loading into PostgreSQL.
    """

    logger.info("Starting customer transformation | input_rows=%d", len(df))

    # ---------------------------------------------------------
    # 1. Work on a copy
    # ---------------------------------------------------------
    # Prevents changes to the original DataFrame.
    df = df.copy()

    # ---------------------------------------------------------
    # 2. Standardize column names
    # ---------------------------------------------------------
    # Example:
    # " First Name " -> "first_name"
    # "Annual Spend" -> "annual_spend"
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )

    # ---------------------------------------------------------
    # 3. Remove exact duplicate rows
    # ---------------------------------------------------------
    before_duplicates = len(df)

    df = df.drop_duplicates().copy()

    duplicates_removed = before_duplicates - len(df)

    logger.info(
        "Customer duplicates removed | count=%d",
        duplicates_removed
    )

    # ---------------------------------------------------------
    # 4. Clean first name
    # ---------------------------------------------------------
    if "first_name" in df.columns:
        df["first_name"] = (
            df["first_name"]
            .astype("string")
            .str.strip()
            .str.title()
        )

    # ---------------------------------------------------------
    # 5. Clean last name
    # ---------------------------------------------------------
    if "last_name" in df.columns:
        df["last_name"] = (
            df["last_name"]
            .astype("string")
            .str.strip()
            .str.title()
        )

    # ---------------------------------------------------------
    # 6. Clean email
    # ---------------------------------------------------------
    if "email" in df.columns:

        df["email"] = (
            df["email"]
            .astype("string")
            .str.strip()
            .str.lower()
        )

        # Basic email validation
        valid_email = df["email"].str.match(
            r"^[\w\.-]+@[\w\.-]+\.\w+$",
            na=False
        )

        # Invalid emails become NULL
        df.loc[~valid_email, "email"] = pd.NA

    # ---------------------------------------------------------
    # 7. Clean phone
    # ---------------------------------------------------------
    if "phone" in df.columns:
        df["phone"] = (
            df["phone"]
            .astype("string")
            .str.strip()
        )

    # ---------------------------------------------------------
    # 8. Normalize country
    # ---------------------------------------------------------
    if "country" in df.columns:
        df["country"] = (
            df["country"]
            .astype("string")
            .str.strip()
            .str.upper()
        )

    # ---------------------------------------------------------
    # 9. Convert signup_date to datetime
    # ---------------------------------------------------------
    if "signup_date" in df.columns:
        df["signup_date"] = pd.to_datetime(
            df["signup_date"],
            errors="coerce",
            format="mixed"
        )

    # ---------------------------------------------------------
    # 10. Convert annual_spend to numeric
    # ---------------------------------------------------------
    if "annual_spend" in df.columns:
        df["annual_spend"] = pd.to_numeric(
            df["annual_spend"],
            errors="coerce"
        )

    # ---------------------------------------------------------
    # 11. Remove rows missing critical fields
    # ---------------------------------------------------------
    # A customer must have:
    # - first_name
    # - email
    required_columns = ["first_name", "email"]

    existing_required = [
        column for column in required_columns
        if column in df.columns
    ]

    before_required_filter = len(df)

    df = df.dropna(subset=existing_required)

    removed_missing_required = (
        before_required_filter - len(df)
    )

    logger.info(
        "Customers removed for missing required fields | count=%d",
        removed_missing_required
    )

    # ---------------------------------------------------------
    # 12. Remove negative annual spend
    # ---------------------------------------------------------
    if "annual_spend" in df.columns:

        before_spend_filter = len(df)

        df = df[
            df["annual_spend"].isna()
            | (df["annual_spend"] >= 0)
        ]

        removed_invalid_spend = (
            before_spend_filter - len(df)
        )

        logger.info(
            "Customers removed for negative spend | count=%d",
            removed_invalid_spend
        )

    # ---------------------------------------------------------
    # 13. Reset DataFrame index
    # ---------------------------------------------------------
    df = df.reset_index(drop=True)

    logger.info(
        "Customer transformation completed | output_rows=%d",
        len(df)
    )

    return df