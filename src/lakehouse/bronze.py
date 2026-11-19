"""Bronze layer: land the raw CSV files as Delta tables, unchanged.

Rules for bronze:
* keep every column as a string, exactly as the source sent it (no lost data
  if a value fails to parse, we deal with types in silver)
* add lineage columns: which batch, which file, when we loaded it
* re-running the same batch must not create duplicates (replaceWhere)
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.tables import TableStore

log = logging.getLogger(__name__)

ENTITIES = ["patients", "encounters", "conditions", "medications"]


def read_landing_csv(store: TableStore, batch_id: str, entity: str) -> DataFrame | None:
    path = store.landing(batch_id, entity)
    try:
        df = (
            store.spark.read.option("header", "true")
            # no inferSchema: every column stays a string in bronze
            .option("mode", "PERMISSIVE")
            .csv(path)
        )
        df.schema  # forces Spark to look at the file now, so a missing file fails here
    except Exception as exc:  # AnalysisException: path does not exist
        if "PATH_NOT_FOUND" in str(exc) or "does not exist" in str(exc):
            log.warning("bronze: %s not found for %s, skipping", entity, batch_id)
            return None
        raise
    return df


def add_lineage(df: DataFrame, batch_id: str) -> DataFrame:
    return (
        df.withColumn("_batch_id", F.lit(batch_id))
        # _metadata is a hidden column every file source has (works on Databricks + OSS Spark)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def ingest_entity(store: TableStore, batch_id: str, entity: str) -> int:
    raw = read_landing_csv(store, batch_id, entity)
    if raw is None:
        return 0

    df = add_lineage(raw, batch_id)
    table = f"bronze_{entity}"

    if store.exists(table):
        # Replace only this batch's rows: running the job twice gives the same result.
        store.write(df, table, mode="overwrite",
                    replace_where=f"_batch_id = '{batch_id}'", merge_schema=True)
    else:
        store.write(df, table, mode="overwrite", partition_by=["_batch_id"])

    count = store.read(table).where(F.col("_batch_id") == batch_id).count()
    log.info("bronze: %-12s batch=%s rows=%d", entity, batch_id, count)
    return count


def run(store: TableStore, batch_id: str) -> dict[str, int]:
    store.ensure_schema()
    return {entity: ingest_entity(store, batch_id, entity) for entity in ENTITIES}
