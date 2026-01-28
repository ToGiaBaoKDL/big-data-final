#!/bin/bash
set -e

# Function to create user and database
create_user_and_db() {
    local user=$1
    local password=$2
    local database=$3

    echo "Creating user '$user' and database '$database'..."
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
        CREATE USER $user WITH PASSWORD '$password';
        CREATE DATABASE $database;
        GRANT ALL PRIVILEGES ON DATABASE $database TO $user;
        -- Grant schema privileges for Postgres 15+ defaults
        \c $database
        GRANT ALL ON SCHEMA public TO $user;
EOSQL
}

# Create Airflow DB
create_user_and_db "$AIRFLOW_POSTGRES_USER" "$AIRFLOW_POSTGRES_PASSWORD" "$AIRFLOW_POSTGRES_DB"

# Create MLflow DB
create_user_and_db "$MLFLOW_POSTGRES_USER" "$MLFLOW_POSTGRES_PASSWORD" "$MLFLOW_POSTGRES_DB"

# Create Metabase DB
create_user_and_db "$METABASE_POSTGRES_USER" "$METABASE_POSTGRES_PASSWORD" "$METABASE_POSTGRES_DB"

echo "Database initialization complete."
