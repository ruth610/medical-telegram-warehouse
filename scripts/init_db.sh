#!/bin/bash
# Initialize PostgreSQL database schema

echo "Initializing database..."

# Wait for PostgreSQL to be ready
until pg_isready -h ${POSTGRES_HOST:-localhost} -p ${POSTGRES_PORT:-5432}; do
  echo "Waiting for PostgreSQL..."
  sleep 2
done

echo "PostgreSQL is ready!"

# Create schemas
psql -h ${POSTGRES_HOST:-localhost} -p ${POSTGRES_PORT:-5432} \
     -U ${POSTGRES_USER:-warehouse_user} \
     -d ${POSTGRES_DB:-medical_warehouse} \
     -c "CREATE SCHEMA IF NOT EXISTS raw;"
     
psql -h ${POSTGRES_HOST:-localhost} -p ${POSTGRES_PORT:-5432} \
     -U ${POSTGRES_USER:-warehouse_user} \
     -d ${POSTGRES_DB:-medical_warehouse} \
     -c "CREATE SCHEMA IF NOT EXISTS marts;"

echo "Database initialized!"
