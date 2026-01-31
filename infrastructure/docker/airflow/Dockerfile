ARG AIRFLOW_IMAGE_TAG=2.11.0-python3.11
FROM apache/airflow:${AIRFLOW_IMAGE_TAG}

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY --chown=airflow:root pyproject.toml /tmp/install/

USER airflow

WORKDIR /tmp/install
RUN pip install --no-cache-dir .