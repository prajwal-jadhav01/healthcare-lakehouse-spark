# Interview walkthrough

How to explain this project, and how to make sure you can defend every line of it.

## Rule zero: run it yourself before you mention it
Go through the 7-day plan below. Only put the project on your resume once you have run
every command yourself and can answer the questions in section 4 without notes.
This plan also *is* your Spark learning hour for the week.

| Day | Do | Read | You should be able to say |
|---|---|---|---|
| 1 | `make data`, `make venv`, `make run-local`, `make report` | `README.md`, `jobs/run.py` | "Here's what goes in, what comes out, and in what order" |
| 2 | Open a `pyspark` shell and load `data/lake/bronze_patients` | `bronze.py`, `tables.py` | Why bronze is all strings; how `replaceWhere` makes re-runs safe |
| 3 | Break a row in `data/landing/batch_002/patients.csv` yourself, re-run silver | `quality.py`, `silver.py` (rules part) | How quarantine and the reject-ratio gate work |
| 4 | Look at one patient's history in `make report` | `scd2_merge_patients` | SCD2 MERGE, drawn on paper from memory |
| 5 | Change a gold query, re-run `--layer gold` | `gold.py` | `lead()` window for readmissions; DataFrame API vs SQL |
| 6 | `make up`, trigger the DAG in the Airflow UI, browse MinIO | `docker-compose.yml`, the DAG | Why each layer is its own Airflow task |
| 7 | Databricks Free Edition: deploy, upload, run the job, run `02_explore` | `databricks.yml`, `run_layer.py` | Same code, three runtimes: what changes (only config) |

## 1. The 60-second pitch
> "At work I build ETL in Apache Flink, both streaming and batch. Most data teams I'm applying to
> use Spark and Databricks, so I built a batch lakehouse to show I can carry the same skills over.
> It takes synthetic patient data through bronze, silver and gold Delta tables in PySpark. Silver is
> where the engineering is: data quality rules with a quarantine table, HIPAA-style de-identification
> with salted hashes, SCD Type 2 history on patients done with a single Delta MERGE, and idempotent
> upserts, so re-running a batch is a no-op. The same code runs locally, in Docker with MinIO
> and Airflow, and on Databricks as a Workflow writing Unity Catalog tables."

## 2. The Flink → Spark bridge (have this ready, it *will* come up)
| Flink (what I use at work) | Spark (this project) |
|---|---|
| DataStream / Table API, batch execution mode | DataFrame API / Spark SQL |
| Job graph, operators, parallelism | Logical plan → physical plan (Catalyst), stages, tasks, partitions |
| Keyed state, windows | `Window.partitionBy().orderBy()` + `lead`/`row_number` for batch |
| Sinks writing to Pinot | Delta Lake writer + MERGE |
| Checkpoints for exactly-once | Delta transaction log (ACID commits) + idempotent writes |

Honest line: *"My Spark is newer than my Flink. The concepts carried over directly. What was new was
Delta-specific: MERGE semantics, replaceWhere, deletion vectors, OPTIMIZE."*

## 3. Design decisions: why, not just what
- **Why keep bronze as strings?** If a source sends `"12,5"` as a cost, a typed bronze load drops it or fails.
  Strings keep the raw truth, so silver can be fixed and replayed from bronze without asking the source again.
- **Why `replaceWhere` and not append?** Airflow retries. Append plus a retry gives duplicates.
  `replaceWhere _batch_id = X` atomically swaps out just that batch.
- **Why quarantine and not drop?** Dropped rows are invisible. Quarantine keeps them with the rule names, so
  someone can fix the source. The reject-ratio gate stops a broken feed (say 40% bad) from quietly loading 60% of a day.
- **Why a salted hash and not a plain one?** A plain SHA-256 of a known patient ID can be recomputed
  and looked up. The salt (kept in a secret) prevents that. The same salt gives the same key, so joins still work.
- **The SCD2 MERGE trick, in one sentence:** changed patients appear twice in the source, once with
  `merge_key = patient_key` (matches the current row → close it) and once with `merge_key = NULL`
  (never matches → insert the new version).
- **Why filter unchanged rows before MERGE?** Without it, a re-run rewrote whole files with 0 rows changed
  (you can see it in `DESCRIBE HISTORY`: `numTargetRowsCopied = 69809`). With the left-anti join on
  key + hash, a re-run makes no commit at all.
- **Deletion vectors:** with them on, correcting 1,396 rows copied 0 others. Without them, Delta rewrites
  every file that contains a changed row (copy-on-write).
- **Why full-rebuild gold?** It's around 5K rows from about 170K silver rows, so a full rebuild is seconds and always correct.
  At scale: Change Data Feed on silver, and recompute only the affected months.
- **Why each layer is a separate Airflow task:** retry just the failed layer. Silver has `retries=0`,
  because a data quality failure won't fix itself on retry.
- **Why `max_active_runs=1`:** SCD2 needs batches applied in order. Two batches merging at once could close the wrong version.

## 4. Questions to practise (answer out loud)
1. What's the difference between `repartition` and `coalesce`? Where would you use either here?
2. Why did you set `spark.sql.shuffle.partitions` to 8 locally? What's the default, and when is it wrong?
3. Which operations in `silver.py` cause a shuffle? (window functions, `dropDuplicates`, joins, the MERGE join)
4. What's in a Delta `_delta_log` folder? (JSON commits, checkpoints, `.crc`.) Open one in MinIO and show it.
5. How does time travel work, and what breaks it? (VACUUM below the retention period)
6. What happens if two jobs MERGE into the same table at once? (optimistic concurrency, so one fails with a conflict and retries)
7. Z-ORDER vs partitioning vs liquid clustering: when do you use each?
8. How would you make ingestion incremental on Databricks? (Auto Loader / `cloudFiles`, with checkpointed file tracking)
9. How would you scale this to 1 TB? (cluster sizing, partition sizes of about 128 MB, broadcast joins for small dims, AQE, avoiding UDFs)
10. Walk me through the readmission logic. What edge cases are there? (same-day transfers, death during a stay, open stays with no discharge)

## 5. What you can and can't claim
**Can claim, once you've run it:** PySpark, Spark SQL, Delta Lake (MERGE, time travel, OPTIMIZE/Z-ORDER,
schema evolution, deletion vectors), medallion architecture, SCD Type 2, data quality and quarantine,
Airflow orchestrating Spark, S3-compatible object storage, Databricks (Workflows, Asset Bundles, Unity Catalog, volumes).

**Don't claim:** production Spark, cluster tuning at scale, AWS or Azure services (MinIO ≠ S3 the service),
Lakeflow/DLT, Auto Loader, dbt. Say *"I know where those fit, and here's what I'd change"* (README, last section).

**Where it goes on the resume:** under **Projects**, not Experience. A suggested bullet (edit it to your own words):
> *Healthcare lakehouse (PySpark, Delta Lake, Airflow, Databricks):* batch medallion pipeline over ~170K synthetic
> clinical records, with rule-based quality checks and quarantine, salted-hash de-identification, SCD Type 2 patient
> history via a single Delta MERGE, and idempotent re-runs. The same code runs on MinIO + Airflow and on Databricks Workflows.
