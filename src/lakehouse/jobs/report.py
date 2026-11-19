"""Prints what is in the lakehouse: row counts, SCD2 history, a peek at gold,
and the Delta transaction log (time travel) of silver_patients.

    python -m lakehouse.jobs.report
"""

from __future__ import annotations

from pyspark.sql import functions as F

from lakehouse.config import Config
from lakehouse.spark import get_spark
from lakehouse.tables import TableStore

TABLES = [
    "bronze_patients", "bronze_encounters", "bronze_conditions", "bronze_medications",
    "silver_patients", "silver_encounters", "silver_conditions", "silver_medications",
    "silver_quarantine",
    "gold_readmissions_30d", "gold_condition_cost", "gold_monthly_utilization", "gold_comorbidity_profile",
]


def main() -> None:
    cfg = Config.from_env()
    store = TableStore(get_spark(cfg, "lakehouse-report"), cfg)
    spark = store.spark

    print("\n=== Row counts ===")
    for t in TABLES:
        print(f"  {t:<28} {store.read(t).count() if store.exists(t) else '-':>8}")

    if store.exists("silver_patients"):
        p = store.read("silver_patients")
        print("\n=== SCD2: silver_patients ===")
        print(f"  patients={p.select('patient_key').distinct().count()}  "
              f"current rows={p.where('is_current').count()}  "
              f"closed (historic) rows={p.where('NOT is_current').count()}")
        changed = p.groupBy("patient_key").count().where("count > 1").limit(1).collect()
        if changed:
            print("  history of one patient whose details changed:")
            (p.where(F.col("patient_key") == changed[0]["patient_key"])
             .select(F.substring("patient_key", 1, 12).alias("patient_key"), "marital_status", "city",
                     "zip3", "income", "effective_from", "effective_to", "is_current")
             .orderBy("effective_from").show(truncate=False))

        print("=== Delta history (time travel) of silver_patients ===")
        spark.sql(f"DESCRIBE HISTORY {store.sql_ref('silver_patients')}") \
            .select("version", "timestamp", "operation", "operationMetrics.numOutputRows") \
            .orderBy("version").show(truncate=False)

    if store.exists("silver_quarantine"):
        print("=== Quarantine (bad rows kept for review) ===")
        store.read("silver_quarantine").groupBy("batch_id", "source_table", "failed_rules") \
            .count().orderBy("batch_id", "source_table").show(truncate=False)

    for t, order in [("gold_readmissions_30d", F.col("discharge_month").desc()),
                     ("gold_condition_cost", F.col("total_cost_of_care").desc()),
                     ("gold_comorbidity_profile", F.col("chronic_conditions"))]:
        if store.exists(t):
            print(f"=== {t} ===")
            store.read(t).orderBy(order).show(8, truncate=40)


if __name__ == "__main__":
    main()
