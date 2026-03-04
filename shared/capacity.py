import os
import logging
import requests
from .auth import get_access_token

POWERBI_SCOPE = os.environ.get(
    "POWERBI_SCOPE",
    "https://analysis.windows.net/powerbi/api/.default"
)
CAPACITY_ID = os.environ["CAPACITY_ID"]

def resume_capacity():
    token = get_access_token(POWERBI_SCOPE)
    url = f"https://api.powerbi.com/v1.0/myorg/admin/capacities/{CAPACITY_ID}/resume"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.post(url, headers=headers)
    if resp.status_code not in (200, 202):
        raise Exception(f"Failed to resume capacity: {resp.status_code} {resp.text}")

def pause_capacity():
    token = get_access_token(POWERBI_SCOPE)
    url = f"https://api.powerbi.com/v1.0/myorg/admin/capacities/{CAPACITY_ID}/suspend"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.post(url, headers=headers)
    if resp.status_code not in (200, 202):
        raise Exception(f"Failed to pause capacity: {resp.status_code} {resp.text}")
