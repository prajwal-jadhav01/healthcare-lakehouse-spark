# Airflow + Java + PySpark/Delta in one image. Airflow's BashOperator starts the
# pipeline with `python -m lakehouse.jobs.run`, which runs Spark in local mode
# inside this container.
FROM apache/airflow:2.10.5-python3.12

# Java for Spark, copied from the official Temurin image (no apt needed, works on amd64 + arm64/Apple Silicon)
COPY --from=eclipse-temurin:17-jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY --chown=airflow:root src /opt/lakehouse/src
ENV PYTHONPATH=/opt/lakehouse/src

# Download the Delta + S3A jars at build time, so the first DAG run doesn't wait for Maven.
RUN S3_ENDPOINT=http://warmup:9000 python -c "from lakehouse.config import Config; from lakehouse.spark import get_spark; get_spark(Config.from_env()).stop()"
