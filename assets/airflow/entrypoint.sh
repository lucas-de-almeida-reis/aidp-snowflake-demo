#!/usr/bin/env bash
# Boots Airflow standalone-style: init DB → ensure admin user → scheduler (bg) → webserver (fg).
# Webserver runs in foreground so the container lifecycle is tied to it; scheduler exits
# automatically when its parent process dies.
set -eo pipefail

# Idempotent — safe to re-run on container restart.
airflow db migrate >/dev/null

# Create admin only if it doesn't already exist. The grep target is the username column.
if ! airflow users list 2>/dev/null | awk '{print $1}' | grep -qx "${AIRFLOW_ADMIN_USER:-admin}"; then
  airflow users create \
    --username  "${AIRFLOW_ADMIN_USER:-admin}" \
    --password  "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
    --firstname Demo \
    --lastname  User \
    --role      Admin \
    --email     admin@example.com >/dev/null
fi

airflow scheduler &
exec airflow webserver --port 8080
