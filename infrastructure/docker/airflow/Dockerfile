ARG AIRFLOW_IMAGE_TAG=2.11.0
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

COPY pyproject.toml /tmp/
RUN uv pip install --system --no-cache -r /tmp/pyproject.toml

USER airflow
