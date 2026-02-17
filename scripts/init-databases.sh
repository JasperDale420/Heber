#!/bin/bash
# Creates auxiliary databases required by infrastructure services.
# Mounted into Postgres at /docker-entrypoint-initdb.d/ and executed
# automatically on FIRST INITIALIZATION only (empty data directory).

set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE heber_lakefs;
    CREATE DATABASE heber_apicurio;
EOSQL

echo "All auxiliary databases created successfully."
