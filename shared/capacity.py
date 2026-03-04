import os
import logging
import requests
import time
from .auth import get_access_token

AZURE_MGMT_SCOPE = "https://management.azure.com/.default"
SUBSCRIPTION_ID = os.environ["SUBSCRIPTION_ID"]
RESOURCE_GROUP_NAME = os.environ["RESOURCE_GROUP_NAME"]
CAPACITY_NAME = os.environ["CAPACITY_NAME"]
API_VERSION = "2023-11-01"

def get_capacity_status():
    """Get the current status of the capacity."""
    token = get_access_token(AZURE_MGMT_SCOPE)
    url = f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP_NAME}/providers/Microsoft.Fabric/capacities/{CAPACITY_NAME}?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        raise Exception(f"Failed to get capacity status: {resp.status_code} {resp.text}")
    data = resp.json()
    return data.get("properties", {}).get("state", "Unknown")

def resume_capacity():
    """Resume the capacity with retry logic and status checking."""
    max_retries = 5
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            # Check current status
            status = get_capacity_status()
            logging.info(f"Capacity status: {status}")
            
            if status == "Active":
                logging.info("Capacity is already active")
                return
            
            # Wait before attempting resume
            if attempt > 0:
                wait_time = retry_delay * (attempt + 1)
                logging.info(f"Waiting {wait_time} seconds before retry attempt {attempt + 1}")
                time.sleep(wait_time)
            
            token = get_access_token(AZURE_MGMT_SCOPE)
            url = f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP_NAME}/providers/Microsoft.Fabric/capacities/{CAPACITY_NAME}/resume?api-version={API_VERSION}"
            headers = {"Authorization": f"Bearer {token}"}
            resp = requests.post(url, headers=headers)
            
            if resp.status_code in (200, 202):
                logging.info("Capacity resume request succeeded")
                return
            elif resp.status_code == 400:
                error_msg = resp.text
                if "not ready" in error_msg.lower():
                    logging.warning(f"Service not ready (attempt {attempt + 1}/{max_retries}): {error_msg}")
                    if attempt < max_retries - 1:
                        continue
                raise Exception(f"Failed to resume capacity: {resp.status_code} {resp.text}")
            else:
                raise Exception(f"Failed to resume capacity: {resp.status_code} {resp.text}")
        
        except Exception as e:
            if attempt < max_retries - 1:
                logging.warning(f"Resume attempt {attempt + 1} failed: {e}. Will retry...")
            else:
                raise

def pause_capacity():
    token = get_access_token(AZURE_MGMT_SCOPE)
    url = f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP_NAME}/providers/Microsoft.Fabric/capacities/{CAPACITY_NAME}/suspend?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.post(url, headers=headers)
    if resp.status_code not in (200, 202):
        raise Exception(f"Failed to pause capacity: {resp.status_code} {resp.text}")
