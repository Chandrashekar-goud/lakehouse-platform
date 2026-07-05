"""Structured logging used across ingestion and transform code.

Logs to stdout so that local runs, Airflow task logs, and Databricks driver
logs all capture output. Shipping to CloudWatch would be done by running the
awslogs agent or a CloudWatch Logs handler against these same streams; the
format below is intentionally parse-friendly for that purpose.
"""
import logging
import sys

_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_FORMAT))
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False
    return logger
