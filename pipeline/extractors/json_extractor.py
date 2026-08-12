import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd


logger = logging.getLogger(__name__)


def extract_json(
    file_path: str,
    record_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Extract JSON data from a file and return it as a Pandas DataFrame.

    Parameters
    ----------
    file_path : str
        Path to the JSON file.

    record_path : str, optional
        Dotted path to a nested records array.
        Example: "data.records"

    Returns
    -------
    pd.DataFrame
        Extracted JSON data as a DataFrame.

    Raises
    ------
    FileNotFoundError
        If the JSON file does not exist.

    json.JSONDecodeError
        If the file contains invalid JSON.

    TypeError
        If the JSON root is not a dictionary or list.

    ValueError
        If the JSON structure cannot be normalized.
    """

    path = Path(file_path)

    # --------------------------------------------------
    # 1. Validate file existence
    # --------------------------------------------------
    if not path.exists():
        logger.error(
            f"File not found: {path.absolute()}"
        )
        raise FileNotFoundError(
            f"Missing JSON file: {path}"
        )

    # --------------------------------------------------
    # 2. Read and parse JSON
    # --------------------------------------------------
    try:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)

        # --------------------------------------------------
        # 3. Normalize JSON structure
        # --------------------------------------------------
        if record_path:
            path_list = record_path.split(".")

            df = pd.json_normalize(
                data,
                record_path=path_list
            )

        elif isinstance(data, (dict, list)):
            df = pd.json_normalize(data)

        else:
            raise TypeError(
                f"Unsupported JSON root type: "
                f"{type(data).__name__}"
            )

        # --------------------------------------------------
        # 4. Log extraction result
        # --------------------------------------------------
        logger.info(
            f"Extracted {df.shape[0]} rows and "
            f"{df.shape[1]} columns from JSON {path}"
        )

        return df

    except (
        json.JSONDecodeError,
        TypeError,
        ValueError
    ) as error:

        logger.error(
            f"Failed to parse/normalize JSON "
            f"{path}: {error}"
        )

        raise