"""Gold layer: business tables that a dashboard or analyst reads directly.

Gold is rebuilt from silver on each run (full overwrite). Silver is small
enough here that this is the simplest correct choice; the README explains
when you'd switch to incremental gold.

Two tables use the DataFrame API and two use Spark SQL on purpose, to show both.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from lakehouse.tables import TableStore

log = logging.getLogger(__name__)


# -------------------------------------------------------- DataFrame API
def readmissions_30d(encounters: DataFrame) -> DataFrame:
    """30-day readmission rate per discharge month.

    A readmission = the same patient's next inpatient stay starting within
    30 days of being discharged from the previous one.
    """
    inpatient = encounters.where((F.col("encounter_class") == "inpatient") & F.col("stop_ts").isNotNull())
    by_patient = Window.partitionBy("patient_key").orderBy("start_ts", "encounter_id")

    stays = (
        inpatient
        .withColumn("next_admit_ts", F.lead("start_ts").over(by_patient))
        .withColumn("days_to_next_admit", F.datediff("next_admit_ts", "stop_ts"))
        .withColumn("readmitted_30d",
                    F.coalesce(F.col("days_to_next_admit").between(0, 30), F.lit(False)))
        .withColumn("length_of_stay_days",
                    (F.unix_timestamp("stop_ts") - F.unix_timestamp("start_ts")) / 86400)
    )

    return (
        stays.groupBy(F.date_trunc("month", "stop_ts").cast("date").alias("discharge_month"))
        .agg(
            F.count("*").alias("discharges"),
            F.sum(F.col("readmitted_30d").cast("int")).alias("readmissions_30d"),
            F.round(F.avg("length_of_stay_days"), 2).alias("avg_length_of_stay_days"),
        )
        .withColumn("readmission_rate", F.round(F.col("readmissions_30d") / F.col("discharges"), 4))
        .orderBy("discharge_month")
    )


def condition_cost(encounters: DataFrame, medications: DataFrame) -> DataFrame:
    """Total cost of care per condition = encounter cost + medication cost
    for encounters/prescriptions whose *reason* is that condition."""
    enc = (
        encounters.where(F.col("reason_code").isNotNull())
        .groupBy("reason_code")
        .agg(
            F.first("reason_description", ignorenulls=True).alias("condition"),
            F.count("*").alias("encounters"),
            F.countDistinct("patient_key").alias("patients"),
            F.sum("total_claim_cost").alias("encounter_cost"),
            F.sum("payer_coverage").alias("encounter_payer_covered"),
        )
    )
    meds = (
        medications.where(F.col("reason_code").isNotNull())
        .groupBy("reason_code")
        .agg(F.sum("total_cost").alias("medication_cost"),
             F.count("*").alias("prescriptions"))
    )
    zero = F.lit(0).cast("decimal(18,2)")
    return (
        enc.join(meds, "reason_code", "left")
        .withColumn("medication_cost", F.coalesce("medication_cost", zero))
        .withColumn("prescriptions", F.coalesce("prescriptions", F.lit(0)))
        .withColumn("total_cost_of_care", F.col("encounter_cost") + F.col("medication_cost"))
        .withColumn("cost_per_patient", F.round(F.col("total_cost_of_care") / F.col("patients"), 2))
        .withColumn("payer_coverage_ratio",
                    F.round(F.col("encounter_payer_covered") / F.col("encounter_cost"), 4))
        .drop("encounter_payer_covered")
        .orderBy(F.col("total_cost_of_care").desc())
    )


# -------------------------------------------------------- Spark SQL
MONTHLY_UTILIZATION_SQL = """
SELECT
    CAST(date_trunc('month', start_ts) AS DATE)  AS month,
    encounter_class,
    COUNT(*)                                     AS encounters,
    COUNT(DISTINCT patient_key)                  AS unique_patients,
    SUM(total_claim_cost)                        AS total_claim_cost,
    ROUND(AVG(total_claim_cost), 2)              AS avg_claim_cost
FROM silver_encounters
GROUP BY 1, 2
ORDER BY 1, 2
"""

COMORBIDITY_SQL = """
WITH chronic_groups (pattern, chronic_group) AS (
    VALUES ('%diabetes mellitus%',        'diabetes'),       -- not 'prediabetes'
           ('%hypertension%',             'hypertension'),
           ('%chronic kidney disease%',   'chronic_kidney_disease'),
           ('%chronic obstructive%',      'copd'),
           ('%heart failure%',            'heart_failure'),
           ('%ischemic heart disease%',   'ischemic_heart_disease'),
           ('%asthma%',                   'asthma'),
           ('%hyperlipidemia%',           'hyperlipidemia'),
           ('%obes%',                     'obesity'),
           ('%osteoarthritis%',           'osteoarthritis')
),
patient_chronic AS (
    -- distinct chronic groups each patient has ever been diagnosed with
    SELECT c.patient_key, COUNT(DISTINCT g.chronic_group) AS chronic_count
    FROM silver_conditions c
    JOIN chronic_groups g ON lower(c.description) LIKE g.pattern
    GROUP BY c.patient_key
),
patient_visits AS (
    SELECT patient_key, COUNT(*) AS encounters
    FROM silver_encounters
    GROUP BY patient_key
)
SELECT
    CASE WHEN COALESCE(pc.chronic_count, 0) >= 3 THEN '3+'
         ELSE CAST(COALESCE(pc.chronic_count, 0) AS STRING) END   AS chronic_conditions,
    COUNT(*)                                                     AS patients,
    ROUND(AVG(p.healthcare_expenses), 2)                         AS avg_lifetime_expenses,
    ROUND(AVG(COALESCE(v.encounters, 0)), 1)                     AS avg_encounters,
    ROUND(AVG(CASE WHEN p.is_deceased THEN 1 ELSE 0 END), 4)     AS deceased_share
FROM silver_patients p
LEFT JOIN patient_chronic pc ON p.patient_key = pc.patient_key
LEFT JOIN patient_visits  v  ON p.patient_key = v.patient_key
WHERE p.is_current
GROUP BY 1
ORDER BY 1
"""


def run(store: TableStore) -> dict[str, int]:
    spark = store.spark
    encounters = store.read("silver_encounters")
    medications = store.read("silver_medications")

    # Temp views let the SQL above say "FROM silver_encounters" in every storage mode.
    for name in ["silver_patients", "silver_encounters", "silver_conditions", "silver_medications"]:
        store.read(name).createOrReplaceTempView(name)

    outputs = {
        "gold_readmissions_30d": readmissions_30d(encounters),
        "gold_condition_cost": condition_cost(encounters, medications),
        "gold_monthly_utilization": spark.sql(MONTHLY_UTILIZATION_SQL),
        "gold_comorbidity_profile": spark.sql(COMORBIDITY_SQL),
    }

    counts = {}
    for table, df in outputs.items():
        store.write(df, table, mode="overwrite")
        counts[table] = store.read(table).count()
        log.info("gold:   %-26s rows=%d", table, counts[table])
    return counts
