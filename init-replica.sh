#!/bin/bash
set -e

echo "Waiting for primary (postgres-primary:5432) to become ready..."
until pg_isready -h postgres-primary -p 5432 -U payments_app -d cornflakes_db; do
  sleep 2
done

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  echo "Data directory is empty. Cloning from primary using pg_basebackup..."
  rm -rf "${PGDATA:?}"/*
  PGPASSWORD=replicator_password pg_basebackup \
    -h postgres-primary \
    -p 5432 \
    -U replicator \
    -D "$PGDATA" \
    -Fp \
    -Xs \
    -R \
    -S standby_slot
  chmod 700 "$PGDATA"
  echo "Base backup completed. Standby replica configured."
fi

echo "Starting replica PostgreSQL server..."
exec postgres -c config_file=/etc/postgresql/postgresql.conf
