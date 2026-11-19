"""Creates the SparkSession.

On Databricks a session already exists, so we just return it.
Locally we build one with the Delta Lake extension, and with the S3A
connector when an S3 endpoint (MinIO) is configured.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from lakehouse.config import Config

DELTA_VERSION = "4.0.1"   # must match the delta-spark pip package
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.4.1"  # same Hadoop version PySpark 4.0 ships with


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(cfg: Config, app_name: str = "healthcare-lakehouse") -> SparkSession:
    if on_databricks():
        return SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master(cfg.spark_master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Small data on a laptop: a few shuffle partitions is much faster than the default 200.
        .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "UTC")
        # Deletion vectors: MERGE/UPDATE mark changed rows as deleted instead of rewriting
        # whole Parquet files. Databricks turns this on by default; OSS Delta needs it set.
        .config("spark.databricks.delta.properties.defaults.enableDeletionVectors", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
    )

    extra_packages = []
    if cfg.s3_endpoint:
        extra_packages.append(HADOOP_AWS)
        builder = (
            builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", cfg.s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", cfg.s3_access_key or "")
            .config("spark.hadoop.fs.s3a.secret.key", cfg.s3_secret_key or "")
            # MinIO serves buckets as http://host:9000/<bucket>, not <bucket>.host
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled",
                    str(cfg.s3_endpoint.startswith("https")).lower())
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        )

    spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
