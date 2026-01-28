ARG AIRFLOW_IMAGE_TAG
FROM apache/airflow:${AIRFLOW_IMAGE_TAG}

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

ARG SPARK_TAG
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-amazon \
    pyspark==${SPARK_TAG} \
    polars==1.37.1 \
    numpy==2.4.1 \
    faker==40.1.2 \
    dbt-clickhouse==1.9.8 \
    clickhouse-connect==0.10.0 \
    minio==7.2.20
