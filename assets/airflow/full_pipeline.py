from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
import requests
import base64
import hashlib
import datetime
import json
import os
import time
from urllib.parse import urlparse
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend


# ==========================================
# CONFIG — per-env AIDP job keys
# ==========================================
# Two AIDP jobs per env (one per notebook). The jobs themselves are
# configured at the AIDP level with the right AIDP_ENV env var, so
# the DAG only needs the job keys — it doesn't pass env separately.

JOB_KEYS = {
    "dev": {
        "bronze_to_silver": "4c89ba4b-0f3a-4640-bfa6-749515a76402",
        "silver_to_gold":   "f0693668-7009-4a72-a93e-1c16b5aabe35",
    },
    "prod": {
        "bronze_to_silver": "144525e9-f617-4e04-8616-f7d74c7d0f98",
        "silver_to_gold":   "22ca2481-ff11-45a8-9eeb-217a12520904",
    },
}

# Same workspace for both envs — only the job keys differ.
# Overridable via env so deploy.yaml can drive it.
WORKSPACE_ID = os.environ.get(
    "AIDP_WORKSPACE_ID",
    "51abe3fa-37fd-46f9-a76f-7961117f9835",
)
AIDP_REGION = os.environ.get("AIDP_REGION", "sa-saopaulo-1")
ODI_REPORT_WORKFLOW_ID = "8b996467-4a59-40cf-a432-2b88e3cec8e6"


# ==========================================
# OCI SIGNING FUNCTION (AIDP)
# ==========================================

def oci_sign_request(method, url, body=None):
    TENANCY_OCID = os.environ["TENANCY_ID"]
    USER_OCID = os.environ["USER_ID"]
    FINGERPRINT = os.environ["FINGERPRINT"]
    PRIVATE_KEY_PEM = os.environ["PRIVATE_KEY"]

    parsed = urlparse(url)
    host = parsed.netloc
    path = parsed.path + (f"?{parsed.query}" if parsed.query else "")

    date = datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")

    headers = {
        "date": date,
        "host": host
    }

    signing_string = f"(request-target): {method.lower()} {path}\n" \
                     f"host: {host}\n" \
                     f"date: {date}"

    if body:
        body_json = json.dumps(body)
        content_length = str(len(body_json))
        body_hash = hashlib.sha256(body_json.encode()).digest()
        body_hash_b64 = base64.b64encode(body_hash).decode()

        headers.update({
            "content-type": "application/json",
            "content-length": content_length,
            "x-content-sha256": body_hash_b64
        })

        signing_string += f"\ncontent-type: application/json" \
                          f"\ncontent-length: {content_length}" \
                          f"\nx-content-sha256: {body_hash_b64}"
    else:
        body_json = None

    private_key = serialization.load_pem_private_key(
        PRIVATE_KEY_PEM.encode(),
        password=None,
        backend=default_backend()
    )

    signature = private_key.sign(
        signing_string.encode(),
        padding.PKCS1v15(),
        hashes.SHA256()
    )

    signature_b64 = base64.b64encode(signature).decode()
    key_id = f"{TENANCY_OCID}/{USER_OCID}/{FINGERPRINT}"

    signed_headers = "(request-target) host date"
    if body:
        signed_headers += " content-type content-length x-content-sha256"

    authorization_header = (
        f'Signature version="1",'
        f'keyId="{key_id}",'
        f'algorithm="rsa-sha256",'
        f'headers="{signed_headers}",'
        f'signature="{signature_b64}"'
    )

    headers["Authorization"] = authorization_header

    return headers, body_json


# ==========================================
# TASKS — AIDP trigger / wait (parameterised)
# ==========================================

def trigger_aidp_job(job_key, xcom_key, **context):
    AIDP_ID = os.environ["AIDP_ID"]
    url = (
        f"https://aidp.{AIDP_REGION}.oci.oraclecloud.com/20240831/"
        f"dataLakes/{AIDP_ID}/workspaces/{WORKSPACE_ID}/jobRuns"
    )
    body = {"jobKey": job_key}

    headers, body_json = oci_sign_request("post", url, body)
    response = requests.post(url, headers=headers, data=body_json)

    if response.status_code != 201:
        raise AirflowException(
            f"Failed to start job {job_key}: {response.text}"
        )

    job_run_key = response.json()["key"]
    context["ti"].xcom_push(key=xcom_key, value=job_run_key)


def wait_for_aidp_job(xcom_key, **context):
    AIDP_ID = os.environ["AIDP_ID"]
    job_run_key = context["ti"].xcom_pull(key=xcom_key)

    url = (
        f"https://aidp.{AIDP_REGION}.oci.oraclecloud.com/20240831/"
        f"dataLakes/{AIDP_ID}/workspaces/{WORKSPACE_ID}/jobRuns/{job_run_key}"
    )

    while True:
        headers, _ = oci_sign_request("get", url)
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            raise AirflowException(f"Failed to get status: {response.text}")

        status = response.json()["state"]["status"]

        if status == "SUCCESS":
            return

        if status in ["FAILED", "CANCELED"]:
            raise AirflowException(f"AIDP Job {job_run_key} ended in {status}")

        time.sleep(30)


# ==========================================
# TASKS — ODI report workflow
# ==========================================

def get_odi_token(**context):
    ODI_BASE_URL = os.environ["ODI_BASE_URL"]

    url = f"{ODI_BASE_URL}/odi/broker/pdbcs/public/v1/token"

    body = {
        "username": os.environ["ODI_USERNAME"],
        "password": os.environ["ODI_PASSWORD"],
        "tenant_name": os.environ["ODI_TENANCY"],
        "database_name": os.environ["ODI_DATABASE_NAME"],
        "cloud_database_name": os.environ["ODI_CLOUD_DB_NAME"],
        "grant_type": "password"
    }

    response = requests.post(url, json=body)

    if response.status_code != 200:
        raise AirflowException(f"Failed to get ODI token: {response.text}")

    access_token = response.json()["access_token"]
    context["ti"].xcom_push(key="odi_token", value=access_token)


def submit_report(**context):
    ODI_BASE_URL = os.environ["ODI_BASE_URL"]
    token = context["ti"].xcom_pull(key="odi_token")

    url = f"{ODI_BASE_URL}/odi/dt-rest/v2/jobs/submit"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    body = {
        "action": "RUN",
        "objectType": "WORKFLOW",
        "objectId": ODI_REPORT_WORKFLOW_ID,
        "objectName": "wf01",
        "synchronous": False,
        "ignorePreviousRunningJob": True,
        "jobName": "airflow-report",
        "jobVariables": {}
    }

    response = requests.post(url, headers=headers, json=body)

    if response.status_code not in [200, 201]:
        raise AirflowException(f"Failed to submit report: {response.text}")

    job_id = response.json()["jobId"]
    context["ti"].xcom_push(key="odi_job_id", value=job_id)


def wait_for_report(**context):
    ODI_BASE_URL = os.environ["ODI_BASE_URL"]
    token = context["ti"].xcom_pull(key="odi_token")
    job_id = context["ti"].xcom_pull(key="odi_job_id")

    url = f"{ODI_BASE_URL}/odi/dt-rest/v2/jobs"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    while True:
        response = requests.post(url, headers=headers, json={})

        if response.status_code != 200:
            raise AirflowException(f"Failed to fetch ODI jobs: {response.text}")

        jobs = response.json()

        for job in jobs:
            if job["jobId"] == job_id:
                status = job["status"]

                if status == "DONE":
                    return

                if status in ["ERROR", "CANCELLED"]:
                    raise AirflowException(f"ODI Job failed: {status}")

        time.sleep(30)


# ==========================================
# DAG FACTORY — one per env
# ==========================================

def make_pipeline_dag(env):
    keys = JOB_KEYS[env]

    with DAG(
        dag_id=f"rappi_medallion_{env}",
        start_date=days_ago(1),
        schedule_interval=None,
        catchup=False,
        tags=["oci", "aidp", "odi", env],
    ) as dag:
        trigger_silver = PythonOperator(
            task_id="trigger_bronze_to_silver",
            python_callable=trigger_aidp_job,
            op_kwargs={
                "job_key": keys["bronze_to_silver"],
                "xcom_key": "bronze_run_key",
            },
        )
        wait_silver = PythonOperator(
            task_id="wait_bronze_to_silver",
            python_callable=wait_for_aidp_job,
            op_kwargs={"xcom_key": "bronze_run_key"},
        )
        trigger_gold = PythonOperator(
            task_id="trigger_silver_to_gold",
            python_callable=trigger_aidp_job,
            op_kwargs={
                "job_key": keys["silver_to_gold"],
                "xcom_key": "gold_run_key",
            },
        )
        wait_gold = PythonOperator(
            task_id="wait_silver_to_gold",
            python_callable=wait_for_aidp_job,
            op_kwargs={"xcom_key": "gold_run_key"},
        )
        get_token = PythonOperator(
            task_id="get_odi_token",
            python_callable=get_odi_token,
        )
        submit_rpt = PythonOperator(
            task_id="submit_report_workflow",
            python_callable=submit_report,
        )
        wait_rpt = PythonOperator(
            task_id="wait_for_report_completion",
            python_callable=wait_for_report,
        )

        (
            trigger_silver
            >> wait_silver
            >> trigger_gold
            >> wait_gold
            >> get_token
            >> submit_rpt
            >> wait_rpt
        )

    return dag


# Two DAGs — one per env. Airflow picks them up from module globals.
rappi_medallion_dev = make_pipeline_dag("dev")
rappi_medallion_prod = make_pipeline_dag("prod")
