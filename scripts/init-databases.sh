#!/bin/bash
# Runs once, the first time the postgres volume is created.
#
# Database-per-service is a hard rule in microservices: a service owns its schema
# and nobody else may read it. Locally we save RAM by putting the three databases
# in one Postgres instance — but they are still three separate databases, so the
# "just join across services" shortcut is physically unavailable.
set -euo pipefail

for db in "${ORDERS_DB:-orders_db}" "${INVENTORY_DB:-inventory_db}" "${NOTIFICATIONS_DB:-notifications_db}"; do
  echo "creating database ${db}"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-EOSQL
    CREATE DATABASE "${db}" OWNER "${POSTGRES_USER}";
EOSQL
done
