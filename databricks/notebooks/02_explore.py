# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Explore the lakehouse
# MAGIC Queries to show in a demo or interview: gold tables, SCD2 history, quarantine,
# MAGIC and Delta time travel. Run after batch_001 and batch_002 have gone through the job.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE workspace.lakehouse;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md ### 30-day readmission rate, last 24 months

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_readmissions_30d ORDER BY discharge_month DESC LIMIT 24

# COMMAND ----------

# MAGIC %md ### Most expensive conditions (encounters + medications)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT condition, patients, total_cost_of_care, cost_per_patient, payer_coverage_ratio
# MAGIC FROM gold_condition_cost ORDER BY total_cost_of_care DESC LIMIT 15

# COMMAND ----------

# MAGIC %md ### Patients with more chronic conditions cost more and visit more

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_comorbidity_profile ORDER BY chronic_conditions

# COMMAND ----------

# MAGIC %md ### SCD Type 2: patients whose details changed in batch_002

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT left(patient_key, 12) AS patient, marital_status, city, zip3, income,
# MAGIC        effective_from, effective_to, is_current
# MAGIC FROM silver_patients
# MAGIC WHERE patient_key IN (SELECT patient_key FROM silver_patients WHERE NOT is_current)
# MAGIC ORDER BY patient_key, effective_from
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md ### Rows rejected by data quality rules

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT batch_id, source_table, failed_rules, raw_record FROM silver_quarantine

# COMMAND ----------

# MAGIC %md ### Delta time travel: the table's history, and the table as it was before batch_002

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver_encounters

# COMMAND ----------

# MAGIC %sql
# MAGIC -- version 0 = the first load (batch_001), before any corrections arrived
# MAGIC SELECT 'now' AS version, SUM(total_claim_cost) AS total FROM silver_encounters
# MAGIC UNION ALL
# MAGIC SELECT 'v0', SUM(total_claim_cost) FROM silver_encounters VERSION AS OF 0
