import os
import logging
import requests
from .auth import get_access_token

AZURE_MGMT_SCOPE = "https://management.azure.com/.default"
SUBSCRIPTION_ID = os.environ["SUBSCRIPTION_ID"]
RESOURCE_GROUP_NAME = os.environ["RESOURCE_GROUP_NAME"]
CAPACITY_NAME = os.environ["CAPACITY_NAME"]
API_VERSION = "2023-11-01"

def resume_capacity():
    token = get_access_token(AZURE_MGMT_SCOPE)
    url = f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP_NAME}/providers/Microsoft.Fabric/capacities/{CAPACITY_NAME}/resume?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.post(url, headers=headers)
    if resp.status_code not in (200, 202):
        raise Exception(f"Failed to resume capacity: {resp.status_code} {resp.text}")

def pause_capacity():
    token = get_access_token(AZURE_MGMT_SCOPE)
    url = f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP_NAME}/providers/Microsoft.Fabric/capacities/{CAPACITY_NAME}/suspend?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.post(url, headers=headers)
    if resp.status_code not in (200, 202):
        raise Exception(f"Failed to pause capacity: {resp.status_code} {resp.text}")
