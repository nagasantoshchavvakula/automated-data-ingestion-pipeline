import logging
import time

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


logger = logging.getLogger(__name__)


def extract_api(
    url: str,
    params: dict | None = None,
    timeout: int = 10
) -> pd.DataFrame:
    """
    Fetch JSON data from an API and return it as a Pandas DataFrame.

    Args:
        url: API endpoint URL.
        params: Optional query parameters.
        timeout: Maximum number of seconds to wait for the response.

    Returns:
        Pandas DataFrame containing the API response.

    Raises:
        requests.exceptions.RequestException:
            If the API request fails.
        ValueError:
            If the API response is not valid JSON.
    """

    session = requests.Session()

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=True
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy
    )

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    start_time = time.perf_counter()

    try:
        logger.info("Fetching API data from %s", url)

        response = session.get(
            url,
            params=params,
            timeout=timeout
        )

        elapsed = time.perf_counter() - start_time

        logger.info(
            "API response received | url=%s | status=%s | elapsed=%.2fs",
            url,
            response.status_code,
            elapsed
        )

        response.raise_for_status()

        data = response.json()

        if isinstance(data, list):
            df = pd.json_normalize(data)

        elif isinstance(data, dict):
            df = pd.json_normalize(data)

        else:
            raise TypeError(
                f"Unsupported API response type: {type(data).__name__}"
            )

        logger.info(
            "API extraction successful | rows=%s | columns=%s",
            df.shape[0],
            df.shape[1]
        )

        return df

    except requests.exceptions.RequestException as exc:
        logger.error(
            "API request failed | url=%s | error=%s",
            url,
            exc
        )
        raise

    except ValueError as exc:
        logger.error(
            "Failed to parse API JSON response | url=%s | error=%s",
            url,
            exc
        )
        raise

    finally:
        session.close()