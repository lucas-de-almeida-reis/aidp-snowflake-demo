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
# TASK 1 - TRIGGER AIDP JOB
# ==========================================

def trigger_job(**context):
    AIDP_ID = os.environ["AIDP_ID"]

    url = f"https://aidp.sa-saopaulo-1.oci.oraclecloud.com/20240831/" \
          f"dataLakes/{AIDP_ID}/workspaces/" \
          f"51abe3fa-37fd-46f9-a76f-7961117f9835/jobRuns"

    body = {"jobKey": "7b71aac0-6ecc-4a9a-a06d-0c439775ffed"}

    headers, body_json = oci_sign_request("post", url, body)
    response = requests.post(url, headers=headers, data=body_json)

    if response.status_code != 201:
        raise AirflowException(f"Failed to start job: {response.text}")

    job_run_key = response.json()["key"]
    context["ti"].xcom_push(key="job_run_key", value=job_run_key)


# ==========================================
# TASK 2 - WAIT AIDP COMPLETION
# ==========================================

def wait_for_job(**context):
    AIDP_ID = os.environ["AIDP_ID"]
    job_run_key = context["ti"].xcom_pull(key="job_run_key")

    url = f"https://aidp.sa-saopaulo-1.oci.oraclecloud.com/20240831/" \
          f"dataLakes/{AIDP_ID}/workspaces/" \
          f"51abe3fa-37fd-46f9-a76f-7961117f9835/jobRuns/{job_run_key}"

    while True:
        headers, _ = oci_sign_request("get", url)
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            raise AirflowException(f"Failed to get status: {response.text}")

        status = response.json()["state"]["status"]

        if status == "SUCCESS":
            return

        if status in ["FAILED", "CANCELED"]:
            raise AirflowException(f"AIDP Job failed: {status}")

        time.sleep(30)


# ==========================================
# TASK 3 - GET ODI TOKEN
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


# ==========================================
# TASK 4 - SUBMIT REPORT WORKFLOW
# ==========================================

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
        "objectId": "8b996467-4a59-40cf-a432-2b88e3cec8e6",
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


# ==========================================
# TASK 5 - WAIT REPORT COMPLETION
# ==========================================

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
# DAG DEFINITION
# ==========================================

with DAG(
    dag_id="full_pipeline",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["oci", "aidp", "odi"]
) as dag:

    t1 = PythonOperator(task_id="trigger_job", python_callable=trigger_job)
    t2 = PythonOperator(task_id="wait_for_completion", python_callable=wait_for_job)
    t3 = PythonOperator(task_id="get_odi_token", python_callable=get_odi_token)
    t4 = PythonOperator(task_id="submit_report_workflow", python_callable=submit_report)
    t5 = PythonOperator(task_id="wait_for_report_completion", python_callable=wait_for_report)

    t1 >> t2 >> t3 >> t4 >> t5
#    t3 >> t4 >> t5
