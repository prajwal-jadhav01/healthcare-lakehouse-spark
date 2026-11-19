# Local (no Docker) workflow needs: Python 3.10-3.13 and Java 17 or 21.
PY      ?= python3
VENV    ?= .venv
RUN      = PYTHONPATH=src $(VENV)/bin/python

.PHONY: help venv data run-local report test clean up down airflow-run minio-sync logs bundle-deploy bundle-run

help:
	@grep -E '^[a-z-]+:.*## ' Makefile | awk -F':.*## ' '{printf "  make %-14s %s\n", $$1, $$2}'

venv: ## create .venv with PySpark + Delta + pytest
	$(PY) -m venv $(VENV) && $(VENV)/bin/pip install -q -r requirements-dev.txt

data: ## generate synthetic patients with Synthea (needs Docker) -> data/landing
	./scripts/generate_data.sh

run-local: ## run batch_001 then batch_002 through all layers, on local disk
	$(RUN) -m lakehouse.jobs.run --layer all --batch-id batch_001
	$(RUN) -m lakehouse.jobs.run --layer all --batch-id batch_002

report: ## print row counts, SCD2 history, quarantine, gold tables
	$(RUN) -m lakehouse.jobs.report

test: ## run unit tests (local Spark)
	$(RUN) -m pytest -q

clean: ## delete the local lake (keeps landing data)
	rm -rf data/lake spark-warehouse metastore_db derby.log

# ---- Docker: MinIO + Airflow -------------------------------------------
up: ## start MinIO + Airflow (first build takes a few minutes)
	docker compose up -d --build

minio-sync: ## copy data/landing into the MinIO bucket again (after `make data`)
	docker compose run --rm minio-init

airflow-run: ## trigger the DAG for BATCH (default batch_001) and wait for it
	docker compose exec airflow airflow dags test healthcare_lakehouse -c '{"batch_id": "$(or $(BATCH),batch_001)"}'

logs:
	docker compose logs -f airflow

down: ## stop containers and delete their volumes
	docker compose down -v

# ---- Databricks Free Edition ---------------------------------------------
bundle-deploy: ## deploy the job to your Databricks workspace
	databricks bundle deploy -t dev

bundle-run: ## run the deployed job for BATCH (default batch_001)
	databricks bundle run -t dev healthcare_lakehouse --params batch_id=$(or $(BATCH),batch_001)
