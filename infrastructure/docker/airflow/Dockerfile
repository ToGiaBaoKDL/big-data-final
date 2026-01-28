ARG AIRFLOW_IMAGE_TAG=3.1.6-python3.11
FROM apache/airflow:${AIRFLOW_IMAGE_TAG}

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER root

ARG SPARK_TAG=3.5.8
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN uv pip install --system --no-cache \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-amazon \
    pyspark==${SPARK_TAG} \
    polars==1.37.1 \
    numpy==2.4.1 \
    faker==40.1.2 \
    dbt-clickhouse==1.9.8 \
    clickhouse-connect==0.10.0 \
    minio==7.2.20 \
    mlflow==3.8.1

USER airflow
