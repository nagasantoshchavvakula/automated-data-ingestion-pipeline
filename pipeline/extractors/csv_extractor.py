import logging
from pathlib import Path

import pandas as pd


logger = logging.getLogger(__name__)


def extract_csv(file_path: str, **kwargs) -> pd.DataFrame:
    """
    Extract a CSV file into a Pandas DataFrame.

    Parameters
    ----------
    file_path : str
        Path to the CSV file.

    **kwargs
        Optional arguments passed to pandas.read_csv().

    Returns
    -------
    pd.DataFrame
        Raw CSV data loaded into memory.

    Raises
    ------
    FileNotFoundError
        If the CSV file does not exist.

    Exception
        Re-raises CSV parsing or reading errors.
    """

    path = Path(file_path)

    # 1. Validate that the file exists
    if not path.exists():
        logger.error(f"File not found: {path.absolute()}")
        raise FileNotFoundError(f"Missing file: {path}")

    try:
        # 2. Set UTF-8 as the default encoding
        encoding = kwargs.pop("encoding", "utf-8")

        # 3. Read the CSV into a DataFrame
        df = pd.read_csv(
            path,
            encoding=encoding,
            **kwargs
        )

        # 4. Log successful extraction
        logger.info(
            f"Extracted {df.shape[0]} rows and "
            f"{df.shape[1]} columns from CSV: {path.absolute()}"
        )

        # 5. Return raw DataFrame
        return df

    except Exception as e:
        logger.error(
            f"Failed to parse CSV {path.absolute()}: {e}"
        )
        raise