#!/bin/sh
set -e

until (mc alias set minio http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"); do 
    echo '...waiting...' && sleep 5
done

BUCKET_LANDING=${MINIO_BUCKET_LANDING}
BUCKET_ANALYTICS=${MINIO_BUCKET_ANALYTICS}
BUCKET_LOGS=${MINIO_BUCKET_LOGS}
BUCKET_MLFLOW=${MINIO_BUCKET_MLFLOW}
BUCKET_DATASCIENCE=${MINIO_BUCKET_DATASCIENCE}

mc mb --ignore-existing minio/$BUCKET_LANDING
mc mb --ignore-existing minio/$BUCKET_ANALYTICS
mc mb --ignore-existing minio/$BUCKET_LOGS
mc mb --ignore-existing minio/$BUCKET_MLFLOW
mc mb --ignore-existing minio/$BUCKET_DATASCIENCE

echo "Buckets created: $BUCKET_LANDING, $BUCKET_ANALYTICS, $BUCKET_LOGS, $BUCKET_MLFLOW, $BUCKET_DATASCIENCE"
exit 0
