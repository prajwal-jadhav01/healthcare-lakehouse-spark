"""Runs one landing batch through bronze -> silver -> gold -> optimize.

Trigger with a batch id, e.g. {"batch_id": "batch_002"}. Each task is a
separate Spark job, so a failed layer can be retried on its own.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

RUN = "python -m lakehouse.jobs.run --layer {layer} --batch-id {{{{ params.batch_id }}}}"

with DAG(
    dag_id="healthcare_lakehouse",
    description="Synthea CSVs -> Delta Lake medallion (bronze/silver/gold)",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # triggered per batch; use "@daily" for a daily drop
    catchup=False,
    max_active_runs=1,      # batches must land in order for SCD2 history to be right
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    params={"batch_id": Param("batch_001", type="string", description="folder under landing/")},
    tags=["spark", "delta", "healthcare"],
) as dag:
    bronze = BashOperator(task_id="bronze", bash_command=RUN.format(layer="bronze"))
    silver = BashOperator(
        task_id="silver",
        bash_command=RUN.format(layer="silver"),
        retries=0,  # a data quality failure won't fix itself on retry
        doc_md="Fails if more than MAX_REJECT_RATIO of a table's rows are quarantined.",
    )
    gold = BashOperator(task_id="gold", bash_command=RUN.format(layer="gold"))
    optimize = BashOperator(task_id="optimize", bash_command=RUN.format(layer="optimize"))

    bronze >> silver >> gold >> optimize
