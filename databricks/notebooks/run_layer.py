# Databricks notebook source
# MAGIC %md
# MAGIC # Run one pipeline layer
# MAGIC Thin wrapper: the real code lives in `src/lakehouse/` and is the **same code** that runs
# MAGIC locally and in Airflow. Here it only switches storage to Unity Catalog tables.
# MAGIC
# MAGIC Widgets: `layer` (bronze / silver / gold / optimize / all), `batch_id`, `catalog`, `schema`.

# COMMAND ----------

dbutils.widgets.text("layer", "all")
dbutils.widgets.text("batch_id", "batch_001")
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "lakehouse")

layer = dbutils.widgets.get("layer")
batch_id = dbutils.widgets.get("batch_id")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

import logging
import os
import sys

# Notebooks run with their own folder as the working directory, so ../../src is the repo's src/.
sys.path.insert(0, os.path.abspath("../../src"))

try:
    # Optional: `databricks secrets create-scope lakehouse` + put-secret phi_hash_salt
    salt = dbutils.secrets.get("lakehouse", "phi_hash_salt")
except Exception:
    salt = "databricks-demo-salt"
    print("No secret scope 'lakehouse' found, using a demo salt for patient id hashing.")

os.environ.update({
    "STORAGE_MODE": "catalog",
    "CATALOG": catalog,
    "SCHEMA": schema,
    "LANDING_ROOT": f"/Volumes/{catalog}/{schema}/landing",
    "PHI_HASH_SALT": salt,
})
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s", force=True)

# COMMAND ----------

from lakehouse.config import Config
from lakehouse.jobs.run import LAYERS, run_layer
from lakehouse.tables import TableStore

store = TableStore(spark, Config.from_env())
for name in (LAYERS if layer == "all" else [layer]):
    run_layer(store, name, batch_id)
