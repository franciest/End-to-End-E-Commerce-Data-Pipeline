"""
spark/spark_session.py

Creates a local SparkSession for the medallion (Bronze/Silver/Gold) jobs.
Deliberately local-only (master="local[*]") and reads/writes plain local
disk — MinIO stays out of Spark's read/write path entirely (see
spark/minio_sync.py for how data moves between MinIO and local disk).
This avoids the fragile hadoop-aws/S3A JAR version-matching that's a
common source of hard-to-debug PySpark errors, especially on Windows.

Usage:
    from spark.spark_session import get_spark
    spark = get_spark("bronze")
    df = spark.read.csv(...)
"""

from pyspark.sql import SparkSession

from utils.logger import get_logger

logger = get_logger(__name__)


def get_spark(app_name: str) -> SparkSession:
    logger.info("Starting Spark session", extra={"app_name": app_name})
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # Keep shuffle partitions low for local/small-data runs — Spark's
        # default of 200 creates a lot of tiny output files for datasets
        # this size, which is wasteful and slow on a single machine.
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
