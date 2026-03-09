from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

WORKSPACE_HOST_CHOICES = {
    1: ("https://insights.ins-try.us-east-1.indico-prod.indico.io/", "ben.francis@indicodata.ai"),
    2: ("https://insights.ins-try-eu.eu-west-2.indico-prod.indico.io/", "ben.francis@indicodata.ai"),
    3: ("https://insights.ins-enrich.us-east-2.indico-dev.indico.io/", "ben.francis@indico.io"),
    4: ("https://insights.ins-aviva.us-east-2.indico-prospect.indico.io/", "ben.francis@indico.io"),
    5: ("https://insights.ins-claims-pinned.us-east-2.indico-dev.indico.io/", "ben.francis@indico.io"),
}

WORKFLOW_HOST_CHOICES = {
    1: ("try.indico.io", "TRY_TOKEN_PATH"),
}


@dataclass(frozen=True)
class Settings:
    workspace_host: str
    workspace_email: str
    workspace_password: str
    github_token: str
    google_api_key: str
    swissre_private_key_path: Path
    swissre_client_id: str
    swissre_token_url: str
    catnet_base_url: str
    dnb_client_id: str
    dnb_client_secret: str
    dnb_token_url: str
    dnb_base_url: str
    workflow_host: str
    workflow_token: Path


def get_settings(workflow_option: int, workspace_option: int) -> Settings:
    try:
        workspace_host, workspace_email = WORKSPACE_HOST_CHOICES[workspace_option]
    except KeyError as exc:
        raise ValueError(f"Invalid host option: {workspace_option}. Valid options: {sorted(WORKSPACE_HOST_CHOICES)}") from exc

    try:
        workflow_host, workflow_token_path = WORKFLOW_HOST_CHOICES[workflow_option]
    except KeyError as exc:
        raise ValueError(f"Invalid host option: {workflow_option}. Valid options: {sorted(WORKFLOW_HOST_CHOICES)}") from exc

    workspace_password = os.getenv("WORKSPACE_PASSWORD")
    github_token = os.getenv("GITHUB_TOKEN")
    google_api_key = os.getenv("G_API")
    catnet_base_url = os.getenv("CATNET_BASE_URL")
    swissre_client_id = os.getenv("SWISSRE_CLIENT_ID")
    swissre_token_url = os.getenv("SWISSRE_TOKEN_URL")
    swissre_private_key_path = Path(os.getenv("SWISSRE_PRIVATE_KEY_PATH", "private.key"))
    dnb_client_id = os.getenv("DNB_CLIENT_ID")
    dnb_client_secret = os.getenv("DNB_CLIENT_SECRET")
    dnb_token_url = os.getenv("DNB_TOKEN_URL")
    dnb_base_url = os.getenv("DNB_BASE_URL")
    workflow_token = os.getenv(workflow_token_path)

    missing = []
    if not workspace_password:
        missing.append("WORKSPACE_PASSWORD")
    if not github_token:
        missing.append("GITHUB_TOKEN")
    if not google_api_key:
        missing.append("G_API")

    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    return Settings(
        workspace_host=workspace_host,
        workspace_email=workspace_email,
        workspace_password=workspace_password,
        github_token=github_token,
        google_api_key=google_api_key,
        swissre_private_key_path=swissre_private_key_path,
        swissre_client_id=swissre_client_id,
        swissre_token_url=swissre_token_url,
        catnet_base_url=catnet_base_url,
        dnb_client_id = dnb_client_id,
        dnb_client_secret = dnb_client_secret,
        dnb_token_url = dnb_token_url,
        dnb_base_url = dnb_base_url,
        workflow_host = workflow_host,
        workflow_token = workflow_token
    )
