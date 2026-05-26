# Single-container demo Airflow.
# Webserver + scheduler in one process, SQLite metastore, SequentialExecutor.
# Fine for this DAG (a linear chain). Not appropriate for production parallel workloads.
FROM apache/airflow:2.9.3-python3.11

USER airflow

# DAG-specific deps. oci pulled in for resource-principal signing of AIDP REST calls.
RUN pip install --no-cache-dir 'oci>=2.130' requests

# DAG + container init
COPY --chown=airflow:0 full_pipeline.py /opt/airflow/dags/full_pipeline.py
COPY --chown=airflow:0 entrypoint.sh    /entrypoint.sh

ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db \
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True \
    AIRFLOW_HOME=/opt/airflow

EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]
