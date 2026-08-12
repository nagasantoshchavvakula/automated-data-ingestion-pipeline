import logging
import sys
from pathlib import Path


def setup_logging() -> None:
    """
    Configure application-wide logging.

    Console:
        INFO and above

    File:
        DEBUG and above
    """

    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / "pipeline.log"

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    # -----------------------------------------------------
    # Console Handler
    # -----------------------------------------------------

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # -----------------------------------------------------
    # File Handler
    # -----------------------------------------------------

    file_handler = logging.FileHandler(
        log_file,
        encoding="utf-8",
    )

    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # -----------------------------------------------------
    # Root Logger
    # -----------------------------------------------------

    logging.basicConfig(
        level=logging.DEBUG,
        handlers=[
            console_handler,
            file_handler,
        ],
        force=True,
    )