import os
import json
import datetime
from azure.storage.blob import BlobClient

def get_watermark_utc() -> datetime.datetime:
    conn_str = os.environ["WATERMARK_STORAGE_CONN_STR"]
    container = os.environ.get("WATERMARK_CONTAINER", "wolf-innovations-prod")
    blob_name = os.environ.get("WATERMARK_BLOB_NAME", "fpl/fpl_next_run.json")

    blob_client = BlobClient.from_connection_string(
        conn_str=conn_str,
        container_name=container,
        blob_name=blob_name
    )

    data = blob_client.download_blob().readall()
    doc = json.loads(data)

    # Expect format: {"nextRunUtc": "2026-03-04T18:00:00Z"}
    value = doc["nextRunUtc"]
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    dt = datetime.datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)