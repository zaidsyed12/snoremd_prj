# Structured logging for the ingestion pipeline.

import logging
import sys
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

_fmt = "%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s"
_datefmt = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # already configured

    logger.setLevel(logging.DEBUG)

    # Console handler — INFO and above
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(_fmt, _datefmt))
    logger.addHandler(ch)

    # File handler — DEBUG and above
    fh = logging.FileHandler(LOG_DIR / "ingestion.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(_fmt, _datefmt))
    logger.addHandler(fh)

    return logger
