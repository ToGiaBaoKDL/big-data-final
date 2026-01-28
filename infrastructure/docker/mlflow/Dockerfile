ARG MLFLOW_PYTHON_TAG=3.11-slim
FROM python:${MLFLOW_PYTHON_TAG}

WORKDIR /app

# Arguments for MLflow version
ARG MLFLOW_VER=3.8.1

# Install MLflow and dependencies
# psycopg2-binary for Postgres backend
# boto3 for S3 artifact storage
RUN pip install --no-cache-dir \
    mlflow==${MLFLOW_VER} \
    psycopg2-binary \
    boto3

EXPOSE 5000

CMD ["mlflow", "server"]
