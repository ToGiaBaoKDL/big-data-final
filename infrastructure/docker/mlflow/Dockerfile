ARG MLFLOW_PYTHON_TAG=3.11-slim
FROM python:${MLFLOW_PYTHON_TAG}

WORKDIR /app

ARG MLFLOW_VER=3.8.1

# Create user for MLflow
RUN useradd -m -u 1000 mlflow

# Switch to mlflow user
USER mlflow

# Install MLflow and dependencies using pip
RUN pip install --no-cache-dir --user \
    mlflow==${MLFLOW_VER} \
    psycopg2-binary \
    boto3

ENV PATH="/home/mlflow/.local/bin:$PATH"

EXPOSE 5000

CMD ["mlflow", "server"]