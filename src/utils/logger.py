"""
utils/logger.py

Central logging setup for the pipeline. Every module (ingestion, processing,
storage, kafka, airflow tasks) should get its logger from here instead of
calling `logging.getLogger` directly — that way log format, level, and
handlers stay consistent project-wide.

Usage:
    from utils.logger import get_logger
    logger = get_logger(__name__)
    logger.info("Starting ingestion for customers table")
"""

import logging
import os
import sys

from pythonjsonlogger import jsonlogger

# Read desired log level from env, default to INFO. Lets you run
# LOG_LEVEL=DEBUG locally without touching code.
_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def get_logger(name: str) -> logging.Logger:
    """
    Return a configured logger for the given module name.

    Logs are emitted as JSON to stdout so they're easy to ship to a log
    aggregator later (CloudWatch, ELK, etc.) without changing this module —
    only the handler destination would need to change, not the call sites.
    """
    logger = logging.getLogger(name)

    # Guard against adding duplicate handlers if get_logger() is called
    # multiple times for the same module (e.g. re-imports in notebooks).
    if logger.handlers:
        return logger

    logger.setLevel(_LOG_LEVEL)

    handler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter(_LOG_FORMAT)
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.propagate = False  # avoid double-logging via the root logger

    return logger
