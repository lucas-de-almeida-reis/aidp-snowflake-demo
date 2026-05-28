#!/usr/bin/env bash
# Boots Airflow standalone-style: init DB → ensure admin user → scheduler (bg) → webserver (fg).
# Webserver runs in foreground so the container lifecycle is tied to it; scheduler exits
# automatically when its parent process dies.
set -eo pipefail

# Container exposes Airflow on a public IP — refuse to boot with default
# creds. Both vars must be set (by deploy.py from config-deploy.yaml).
: "${AIRFLOW_ADMIN_USER:?AIRFLOW_ADMIN_USER not set — refusing to boot}"
: "${AIRFLOW_ADMIN_PASSWORD:?AIRFLOW_ADMIN_PASSWORD not set — refusing to boot}"
if [ "$AIRFLOW_ADMIN_PASSWORD" = "admin" ]; then
  echo "AIRFLOW_ADMIN_PASSWORD is 'admin' — refusing to boot. Set a strong password." >&2
  exit 1
fi

# Idempotent — safe to re-run on container restart.
airflow db migrate >/dev/null

# Create admin only if it doesn't already exist. The grep target is the username column.
if ! airflow users list 2>/dev/null | awk '{print $1}' | grep -qx "$AIRFLOW_ADMIN_USER"; then
  airflow users create \
    --username  "$AIRFLOW_ADMIN_USER" \
    --password  "$AIRFLOW_ADMIN_PASSWORD" \
    --firstname Demo \
    --lastname  User \
    --role      Admin \
    --email     admin@example.com >/dev/null
fi

airflow scheduler &
exec airflow webserver --port 8080
