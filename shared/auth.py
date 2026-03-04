import os
import msal

TENANT_ID = os.environ["TENANT_ID"]
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"

def get_access_token(scope: str) -> str:
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_silent([scope], account=None)
    if not result:
        result = app.acquire_token_for_client(scopes=[scope])
    if "access_token" not in result:
        raise Exception(f"Failed to acquire token: {result.get('error_description')}")
    return result["access_token"]