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
PIPELINE_ITEM_ID = os.environ["PIPELINE_ID"]  # This is the item ID in Fabric

def run_pipeline() -> str:
    """Run a pipeline and return the job instance ID."""
    token = get_access_token(FABRIC_SCOPE)
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{PIPELINE_ITEM_ID}/jobs/instances?jobType=Pipeline"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "executionData": {
            "pipelineName": PIPELINE_ITEM_ID
        }
    }
    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code != 202:
        raise Exception(f"Failed to run pipeline: {resp.status_code} {resp.text}")
    
    # The job instance ID is in the Location header or response
    # Try the response first, then fall back to parsing Location header
    data = resp.json() if resp.text else {}
    job_id = data.get("id")
    
    if not job_id:
        # Extract from Location header if available
        location = resp.headers.get("Location", "")
        if location:
            # Location header format: /v1/workspaces/xxx/items/xxx/jobs/instances/{jobInstanceId}
            parts = location.split("/")
            if parts:
                job_id = parts[-1]
    
    if not job_id:
        raise Exception(f"Could not find job instance ID in response: {data}, headers: {resp.headers}")
    
    logging.info(f"Pipeline run started with job instance ID: {job_id}")
    return job_id

def wait_for_pipeline_success(job_id: str, timeout_seconds: int = 7200, poll_interval: int = 30):
    """Poll pipeline job status until completion."""
    token = get_access_token(FABRIC_SCOPE)
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{PIPELINE_ITEM_ID}/jobs/instances/{job_id}"
    headers = {"Authorization": f"Bearer {token}"}

    start = time.time()
    while time.time() - start < timeout_seconds:
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            raise Exception(f"Failed to get job status: {resp.status_code} {resp.text}")
        
        data = resp.json()
        status = data.get("status")
        logging.info(f"Pipeline job {job_id} response: {data}")
        logging.info(f"Pipeline job {job_id} status: {status}")
        
        # Check for completion indicators
        if status == "Completed" or data.get("endTimeUtc"):
            logging.info(f"Pipeline job completed with status: {status}")
            return status or "Completed"
        
        if status in ("Failed", "Cancelled"):
            logging.error(f"Pipeline job {job_id} failed or was cancelled with status: {status}")
            return status
        
        time.sleep(poll_interval)

    raise TimeoutError(f"Pipeline job {job_id} did not complete in {timeout_seconds} seconds")