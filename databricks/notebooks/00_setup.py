# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · One-time setup
# MAGIC Creates the schema and a Unity Catalog **volume** to hold the landing CSV files.
# MAGIC
# MAGIC After running this, upload the batch folders from your laptop:
# MAGIC * **UI:** Catalog → `workspace` → `lakehouse` → Volumes → `landing` → *Upload to this volume*.
# MAGIC   Create folders `batch_001` and `batch_002` and upload the 4 CSVs into each.
# MAGIC * **CLI:** `databricks fs cp -r data/landing dbfs:/Volumes/workspace/lakehouse/landing`

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "lakehouse")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.landing")
print(f"Upload CSVs to /Volumes/{catalog}/{schema}/landing/<batch_id>/")

# COMMAND ----------

# Check what has been uploaded so far
display(dbutils.fs.ls(f"/Volumes/{catalog}/{schema}/landing/"))
