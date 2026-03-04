import os
import logging
import time
import requests
from .auth import get_access_token

FABRIC_SCOPE = os.environ.get(
    "FABRIC_SCOPE",
    "https://api.fabric.microsoft.com/.default"
)

WORKSPACE_ID = os.environ["WORKSPACE_ID"]
PIPELINE_ID = os.environ["PIPELINE_ID"]

def run_pipeline() -> str:
    token = get_access_token(FABRIC_SCOPE)
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/pipelines/{PIPELINE_ID}/run"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    resp = requests.post(url, headers=headers, json={})
    resp.raise_for_status()
    data = resp.json()
    run_id = data.get("id") or data.get("runId")
    if not run_id:
        raise Exception(f"Could not find run id in response: {data}")
    return run_id

def wait_for_pipeline_success(run_id: str, timeout_seconds: int = 7200, poll_interval: int = 30):
    token = get_access_token(FABRIC_SCOPE)
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/pipelines/{PIPELINE_ID}/runs/{run_id}"
    headers = {"Authorization": f"Bearer {token}"}

    start = time.time()
    while time.time() - start < timeout_seconds:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        status = resp.json().get("status")
        logging.info(f"Pipeline run {run_id} status: {status}")
        if status in ("Succeeded", "Failed", "Cancelled"):
            return status
        time.sleep(poll_interval)

    raise TimeoutError(f"Pipeline run {run_id} did not complete in time")