"""Silver layer: clean, typed, de-identified, de-duplicated tables.

For each entity in a batch:
1. read that batch from bronze
2. run data quality rules -> bad rows go to silver_quarantine
3. cast types, normalise values, replace patient ids with a salted hash
4. MERGE into the silver table (upsert), so re-running a batch is safe

Patients keep full history with SCD Type 2 (a new row each time a tracked
attribute changes, the old row gets an end date).
"""

from __future__ import annotations

import logging

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from lakehouse.quality import Rule, check_reject_ratio, split_valid, to_quarantine
from lakehouse.tables import TableStore

log = logging.getLogger(__name__)

LINEAGE = ["_batch_id", "_source_file", "_ingested_at"]
QUARANTINE = "silver_quarantine"


# ---------------------------------------------------------------- helpers
def _bronze_batch(store: TableStore, entity: str, batch_id: str) -> DataFrame | None:
    table = f"bronze_{entity}"
    if not store.exists(table):
        return None
    return store.read(table).where(F.col("_batch_id") == batch_id)


def _raw_columns(df: DataFrame) -> list[str]:
    return [c for c in df.columns if c not in LINEAGE]


def hash_patient_id(col: Column, salt: str) -> Column:
    """Salted SHA-256 of the source patient id.

    Same input -> same key, so tables still join, but the original id can't be
    read back. The salt stops anyone hashing known ids to look them up.
    """
    return F.sha2(F.concat(F.lit(salt), col), 256)


def _ts(col: str) -> Column:
    return F.to_timestamp(F.col(col))


def _date(col: str) -> Column:
    return F.to_date(F.col(col))


def _money(col: str) -> Column:
    return F.col(col).cast("decimal(18,2)")


def _row_hash(cols: list[str]) -> Column:
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in cols]), 256)


def _quality_gate(store: TableStore, df: DataFrame, rules: list[Rule], source: str,
                  batch_id: str) -> DataFrame:
    """Splits off bad rows into quarantine, fails the job if too many are bad."""
    raw_cols = _raw_columns(df)
    valid, rejected = split_valid(df, rules)
    quarantine = to_quarantine(rejected, source, batch_id, raw_cols)

    if store.exists(QUARANTINE):
        store.write(quarantine, QUARANTINE, mode="overwrite",
                    replace_where=f"batch_id = '{batch_id}' AND source_table = '{source}'")
    else:
        store.write(quarantine, QUARANTINE, mode="overwrite")

    total, bad = df.count(), store.read(QUARANTINE).where(
        (F.col("batch_id") == batch_id) & (F.col("source_table") == source)).count()
    log.info("silver: %-12s batch=%s rows=%d quarantined=%d", source, batch_id, total, bad)
    check_reject_ratio(source, total, bad, store.cfg.max_reject_ratio)
    return valid


def only_new_or_changed(store: TableStore, df: DataFrame, table: str, key: str, hash_col: str,
                        target_filter: str | None = None) -> DataFrame:
    """Drops source rows that are already in the target unchanged.

    Without this, re-running a batch makes MERGE rewrite files even though
    nothing changed. With it, a re-run is a cheap no-op.
    """
    target = store.read(table)
    if target_filter:
        target = target.where(target_filter)
    target = target.select(key, hash_col)
    return df.join(target, on=[key, hash_col], how="left_anti")


def upsert(store: TableStore, df: DataFrame, table: str, key: str) -> None:
    """MERGE on `key`: update rows whose content changed, insert new ones."""
    if not store.exists(table):
        store.write(df, table, mode="overwrite")
        return
    df = only_new_or_changed(store, df, table, key, "_row_hash")
    if df.isEmpty():
        log.info("silver: %s already up to date, nothing to merge", table)
        return
    (
        store.delta(table).alias("t")
        .merge(df.alias("s"), f"t.{key} = s.{key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _latest_per_key(df: DataFrame, key: str, order: list[Column]) -> DataFrame:
    w = Window.partitionBy(key).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


# ---------------------------------------------------------------- patients
def patient_rules() -> list[Rule]:
    # built lazily: Column objects need an active SparkSession
    return [
        Rule("id_not_null", F.col("Id").isNotNull()),
        Rule("birthdate_valid", _date("BIRTHDATE").isNotNull()),
        Rule("birthdate_not_in_future", _date("BIRTHDATE") <= F.current_date()),
        Rule("gender_valid", F.col("GENDER").isin("M", "F")),
        Rule("death_after_birth", F.col("DEATHDATE").isNull() | (_date("DEATHDATE") >= _date("BIRTHDATE"))),
    ]

# Columns we track history for. Changing any of these creates a new SCD2 version.
PATIENT_TRACKED = [
    "birth_year", "is_deceased", "death_year", "gender", "race", "ethnicity", "marital_status",
    "city", "county", "state", "zip3", "income", "healthcare_expenses", "healthcare_coverage",
]

MARITAL = {"M": "married", "S": "single", "D": "divorced", "W": "widowed"}


def transform_patients(df: DataFrame, salt: str) -> DataFrame:
    """De-identification in the spirit of HIPAA Safe Harbor:
    drop names, SSN, licence/passport, street address, lat/lon, birthplace;
    keep only birth YEAR (none at all for 90+), and only the first 3 ZIP digits.
    """
    birth = _date("BIRTHDATE")
    death = _date("DEATHDATE")
    age = F.floor(F.months_between(F.coalesce(death, F.current_date()), birth) / 12)
    marital = F.create_map(*[x for k, v in MARITAL.items() for x in (F.lit(k), F.lit(v))])

    out = df.select(
        hash_patient_id(F.col("Id"), salt).alias("patient_key"),
        F.when(age >= 90, F.lit(None)).otherwise(F.year(birth)).alias("birth_year"),
        death.isNotNull().alias("is_deceased"),
        F.year(death).alias("death_year"),
        F.col("GENDER").alias("gender"),
        F.lower("RACE").alias("race"),
        F.lower("ETHNICITY").alias("ethnicity"),
        F.coalesce(marital[F.col("MARITAL")], F.lit("unknown")).alias("marital_status"),
        F.col("CITY").alias("city"),
        F.col("COUNTY").alias("county"),
        F.col("STATE").alias("state"),
        F.substring(F.lpad("ZIP", 5, "0"), 1, 3).alias("zip3"),
        F.col("INCOME").cast("int").alias("income"),
        _money("HEALTHCARE_EXPENSES").alias("healthcare_expenses"),
        _money("HEALTHCARE_COVERAGE").alias("healthcare_coverage"),
        F.col("_ingested_at").alias("effective_from"),
        F.col("_batch_id"),
    )
    out = out.withColumn("attr_hash", _row_hash(PATIENT_TRACKED))
    # If one batch sends the same patient twice with different values, keep one deterministically.
    return _latest_per_key(out, "patient_key", [F.col("attr_hash").desc()])


def scd2_merge_patients(store: TableStore, updates: DataFrame, table: str = "silver_patients") -> None:
    """SCD Type 2 with a single Delta MERGE.

    Trick: changed patients appear twice in the staged data.
      * once with merge_key = patient_key -> matches the current row, which we close
      * once with merge_key = NULL        -> never matches, so it is inserted as the new current row
    New patients only appear once (merge_key = patient_key, no match -> insert).
    Unchanged patients are filtered out before the MERGE (only_new_or_changed).
    """
    if not store.exists(table):
        initial = (updates.withColumn("effective_to", F.lit(None).cast("timestamp"))
                   .withColumn("is_current", F.lit(True)))
        store.write(initial, table, mode="overwrite")
        return

    updates = only_new_or_changed(store, updates, table, "patient_key", "attr_hash", "is_current")
    if updates.isEmpty():
        log.info("silver: %s already up to date, nothing to merge", table)
        return

    current = store.read(table).where("is_current")
    changed = (updates.alias("u")
               .join(current.alias("c"), "patient_key")
               .where(F.col("u.attr_hash") != F.col("c.attr_hash"))
               .select("u.*"))
    staged = (changed.withColumn("merge_key", F.lit(None).cast("string"))
              .unionByName(updates.withColumn("merge_key", F.col("patient_key"))))

    insert_values = {c: f"s.{c}" for c in updates.columns}
    insert_values.update({"effective_to": "CAST(NULL AS TIMESTAMP)", "is_current": "true"})

    (
        store.delta(table).alias("t")
        .merge(staged.alias("s"), "t.patient_key = s.merge_key AND t.is_current = true")
        .whenMatchedUpdate(condition="t.attr_hash <> s.attr_hash",
                           set={"is_current": "false", "effective_to": "s.effective_from"})
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )


def process_patients(store: TableStore, batch_id: str) -> None:
    bronze = _bronze_batch(store, "patients", batch_id)
    if bronze is None:
        return
    bronze = bronze.dropDuplicates(_raw_columns(bronze))  # exact duplicate rows
    valid = _quality_gate(store, bronze, patient_rules(), "patients", batch_id)
    scd2_merge_patients(store, transform_patients(valid, store.cfg.phi_hash_salt))


# ---------------------------------------------------------------- encounters
def encounter_rules() -> list[Rule]:
    # built lazily: Column objects need an active SparkSession
    return [
        Rule("id_not_null", F.col("Id").isNotNull()),
        Rule("patient_not_null", F.col("PATIENT").isNotNull()),
        Rule("start_valid", _ts("START").isNotNull()),
        Rule("stop_after_start", F.col("STOP").isNull() | (_ts("STOP") >= _ts("START"))),
        Rule("cost_not_negative", F.col("TOTAL_CLAIM_COST").cast("double") >= 0),
    ]


def transform_encounters(df: DataFrame, salt: str) -> DataFrame:
    out = df.select(
        F.col("Id").alias("encounter_id"),
        hash_patient_id(F.col("PATIENT"), salt).alias("patient_key"),
        F.col("ORGANIZATION").alias("organization_id"),
        F.col("PROVIDER").alias("provider_id"),
        F.col("PAYER").alias("payer_id"),
        F.lower("ENCOUNTERCLASS").alias("encounter_class"),
        F.col("CODE").alias("code"),
        F.col("DESCRIPTION").alias("description"),
        _ts("START").alias("start_ts"),
        _ts("STOP").alias("stop_ts"),
        _date("START").alias("encounter_date"),
        _money("BASE_ENCOUNTER_COST").alias("base_cost"),
        _money("TOTAL_CLAIM_COST").alias("total_claim_cost"),
        _money("PAYER_COVERAGE").alias("payer_coverage"),
        F.col("REASONCODE").alias("reason_code"),
        F.col("REASONDESCRIPTION").alias("reason_description"),
        F.col("_batch_id"),
    )
    business = [c for c in out.columns if c != "_batch_id"]
    out = out.withColumn("_row_hash", _row_hash(business))
    return _latest_per_key(out, "encounter_id", [F.col("_row_hash").desc()])


def process_encounters(store: TableStore, batch_id: str) -> None:
    bronze = _bronze_batch(store, "encounters", batch_id)
    if bronze is None:
        return
    valid = _quality_gate(store, bronze, encounter_rules(), "encounters", batch_id)
    upsert(store, transform_encounters(valid, store.cfg.phi_hash_salt), "silver_encounters", "encounter_id")


# ---------------------------------------------------------------- conditions
def condition_rules() -> list[Rule]:
    # built lazily: Column objects need an active SparkSession
    return [
        Rule("patient_not_null", F.col("PATIENT").isNotNull()),
        Rule("code_not_null", F.col("CODE").isNotNull()),
        Rule("start_valid", _date("START").isNotNull()),
        Rule("stop_after_start", F.col("STOP").isNull() | (_date("STOP") >= _date("START"))),
    ]


def transform_conditions(df: DataFrame, salt: str) -> DataFrame:
    # No id column in the source, so the natural key is patient + encounter + code + start.
    out = df.select(
        F.sha2(F.concat_ws("|", "PATIENT", "ENCOUNTER", "CODE", "START"), 256).alias("condition_key"),
        hash_patient_id(F.col("PATIENT"), salt).alias("patient_key"),
        F.col("ENCOUNTER").alias("encounter_id"),
        F.col("SYSTEM").alias("code_system"),
        F.col("CODE").alias("code"),
        F.col("DESCRIPTION").alias("description"),
        _date("START").alias("start_date"),
        _date("STOP").alias("stop_date"),
        F.col("STOP").isNull().alias("is_active"),
        F.col("_batch_id"),
    )
    business = [c for c in out.columns if c != "_batch_id"]
    out = out.withColumn("_row_hash", _row_hash(business))
    return _latest_per_key(out, "condition_key", [F.col("_row_hash").desc()])


def process_conditions(store: TableStore, batch_id: str) -> None:
    bronze = _bronze_batch(store, "conditions", batch_id)
    if bronze is None:
        return
    valid = _quality_gate(store, bronze, condition_rules(), "conditions", batch_id)
    upsert(store, transform_conditions(valid, store.cfg.phi_hash_salt), "silver_conditions", "condition_key")


# ---------------------------------------------------------------- medications
def medication_rules() -> list[Rule]:
    # built lazily: Column objects need an active SparkSession
    return [
        Rule("patient_not_null", F.col("PATIENT").isNotNull()),
        Rule("code_not_null", F.col("CODE").isNotNull()),
        Rule("start_valid", _ts("START").isNotNull()),
        Rule("cost_not_negative", F.col("TOTALCOST").cast("double") >= 0),
    ]


def transform_medications(df: DataFrame, salt: str) -> DataFrame:
    out = df.select(
        F.sha2(F.concat_ws("|", "PATIENT", "ENCOUNTER", "CODE", "START"), 256).alias("medication_key"),
        hash_patient_id(F.col("PATIENT"), salt).alias("patient_key"),
        F.col("ENCOUNTER").alias("encounter_id"),
        F.col("PAYER").alias("payer_id"),
        F.col("CODE").alias("code"),
        F.col("DESCRIPTION").alias("description"),
        _ts("START").alias("start_ts"),
        _ts("STOP").alias("stop_ts"),
        _money("BASE_COST").alias("base_cost"),
        _money("PAYER_COVERAGE").alias("payer_coverage"),
        F.col("DISPENSES").cast("int").alias("dispenses"),
        _money("TOTALCOST").alias("total_cost"),
        F.col("REASONCODE").alias("reason_code"),
        F.col("REASONDESCRIPTION").alias("reason_description"),
        F.col("_batch_id"),
    )
    business = [c for c in out.columns if c != "_batch_id"]
    out = out.withColumn("_row_hash", _row_hash(business))
    return _latest_per_key(out, "medication_key", [F.col("_row_hash").desc()])


def process_medications(store: TableStore, batch_id: str) -> None:
    bronze = _bronze_batch(store, "medications", batch_id)
    if bronze is None:
        return
    valid = _quality_gate(store, bronze, medication_rules(), "medications", batch_id)
    upsert(store, transform_medications(valid, store.cfg.phi_hash_salt), "silver_medications", "medication_key")


def run(store: TableStore, batch_id: str) -> None:
    process_patients(store, batch_id)
    process_encounters(store, batch_id)
    process_conditions(store, batch_id)
    process_medications(store, batch_id)
