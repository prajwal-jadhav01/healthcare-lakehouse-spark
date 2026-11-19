"""One place that knows where Delta tables live.

Pipeline code asks for a table by logical name ("silver_patients") and
never cares whether it is a folder on disk / S3 or a Unity Catalog table.
"""

from __future__ import annotations

import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from lakehouse.config import Config


class TableStore:
    def __init__(self, spark: SparkSession, cfg: Config):
        self.spark = spark
        self.cfg = cfg

    # ---- naming -------------------------------------------------------
    @staticmethod
    def _root(root: str) -> str:
        # Plain local folders become absolute paths (Spark SQL rejects relative ones);
        # URIs like s3a://, dbfs:/ or /Volumes/... are left alone.
        root = root.rstrip("/")
        return root if ":" in root or root.startswith("/") else os.path.abspath(root)

    def path(self, name: str) -> str:
        return f"{self._root(self.cfg.lake_root)}/{name}"

    def full_name(self, name: str) -> str:
        return f"{self.cfg.catalog}.{self.cfg.schema}.{name}"

    def sql_ref(self, name: str) -> str:
        """How to refer to the table inside a SQL string."""
        if self.cfg.storage_mode == "catalog":
            return self.full_name(name)
        return f"delta.`{self.path(name)}`"

    def landing(self, batch_id: str, entity: str) -> str:
        return f"{self._root(self.cfg.landing_root)}/{batch_id}/{entity}.csv"

    # ---- read / write -------------------------------------------------
    def exists(self, name: str) -> bool:
        if self.cfg.storage_mode == "catalog":
            return self.spark.catalog.tableExists(self.full_name(name))
        return DeltaTable.isDeltaTable(self.spark, self.path(name))

    def read(self, name: str) -> DataFrame:
        if self.cfg.storage_mode == "catalog":
            return self.spark.read.table(self.full_name(name))
        return self.spark.read.format("delta").load(self.path(name))

    def delta(self, name: str) -> DeltaTable:
        if self.cfg.storage_mode == "catalog":
            return DeltaTable.forName(self.spark, self.full_name(name))
        return DeltaTable.forPath(self.spark, self.path(name))

    def write(
        self,
        df: DataFrame,
        name: str,
        mode: str = "overwrite",
        replace_where: str | None = None,
        partition_by: list[str] | None = None,
        merge_schema: bool = False,
    ) -> None:
        writer = df.write.format("delta").mode(mode)
        if replace_where:
            # Overwrite only the rows matching the predicate -> safe, idempotent re-runs.
            writer = writer.option("replaceWhere", replace_where)
        if merge_schema:
            # Schema evolution: new columns in the source get added to the table.
            writer = writer.option("mergeSchema", "true")
        elif mode == "overwrite" and not replace_where:
            writer = writer.option("overwriteSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        if self.cfg.storage_mode == "catalog":
            writer.saveAsTable(self.full_name(name))
        else:
            writer.save(self.path(name))

    def ensure_schema(self) -> None:
        if self.cfg.storage_mode == "catalog":
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.cfg.catalog}.{self.cfg.schema}")
