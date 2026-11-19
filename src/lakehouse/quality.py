"""Small rule-based data quality checks.

Each rule is a name plus a Column that is TRUE for a good row.
Rows failing any rule are split off into a quarantine table (with the list of
rules they failed and the raw record as JSON), instead of silently dropped
or crashing the whole job.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


class DataQualityError(RuntimeError):
    pass


@dataclass(frozen=True)
class Rule:
    name: str
    is_valid: Column


def split_valid(df: DataFrame, rules: list[Rule]) -> tuple[DataFrame, DataFrame]:
    """Returns (valid_rows, rejected_rows). rejected_rows has a `failed_rules` array."""
    # A rule that evaluates to NULL (e.g. a NULL compared to something) counts as failed.
    failures = [F.when(F.coalesce(r.is_valid, F.lit(False)), F.lit(None)).otherwise(F.lit(r.name))
                for r in rules]
    checked = df.withColumn("failed_rules", F.array_compact(F.array(*failures)))
    valid = checked.where(F.size("failed_rules") == 0).drop("failed_rules")
    rejected = checked.where(F.size("failed_rules") > 0)
    return valid, rejected


def to_quarantine(rejected: DataFrame, source_table: str, batch_id: str,
                  raw_columns: list[str]) -> DataFrame:
    return rejected.select(
        F.lit(source_table).alias("source_table"),
        F.lit(batch_id).alias("batch_id"),
        F.col("failed_rules"),
        F.to_json(F.struct(*raw_columns)).alias("raw_record"),
        F.current_timestamp().alias("quarantined_at"),
    )


def check_reject_ratio(source_table: str, total: int, rejected: int, max_ratio: float) -> None:
    if total == 0:
        return
    ratio = rejected / total
    if ratio > max_ratio:
        raise DataQualityError(
            f"{source_table}: {rejected}/{total} rows ({ratio:.1%}) failed quality rules, "
            f"above the {max_ratio:.0%} limit. Check the quarantine table before re-running."
        )
