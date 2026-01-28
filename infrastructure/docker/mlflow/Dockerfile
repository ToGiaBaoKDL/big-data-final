ARG MLFLOW_PYTHON_TAG=3.11-slim
FROM python:${MLFLOW_PYTHON_TAG}

WORKDIR /app

# Arguments for MLflow version
ARG MLFLOW_VER=3.8.1

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install MLflow and dependencies using uv (much faster)
RUN uv pip install --system --no-cache \
    mlflow==${MLFLOW_VER} \
    psycopg2-binary \
    boto3

EXPOSE 5000

CMD ["mlflow", "server"]
