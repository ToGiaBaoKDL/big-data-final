ARG MLFLOW_PYTHON_TAG
FROM python:${MLFLOW_PYTHON_TAG}

WORKDIR /app

# Arguments for MLflow version
ARG MLFLOW_VER

# Install MLflow and dependencies
# psycopg2-binary for Postgres backend
# boto3 for S3 artifact storage
RUN pip install --no-cache-dir \
    mlflow==${MLFLOW_VER} \
    psycopg2-binary \
    boto3

EXPOSE 5000

CMD ["mlflow", "server"]
