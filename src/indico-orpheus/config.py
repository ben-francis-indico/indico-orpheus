from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

HOST_CHOICES = {
    1: ("https://insights.ins-try.us-east-1.indico-prod.indico.io/", "ben.francis@indicodata.ai"),
    2: ("https://insights.ins-try-eu.eu-west-2.indico-prod.indico.io/", "ben.francis@indicodata.ai"),
    3: ("https://insights.ins-enrich.us-east-2.indico-dev.indico.io/", "ben.francis@indico.io"),
    4: ("https://insights.ins-aviva.us-east-2.indico-prospect.indico.io/", "ben.francis@indico.io"),
    5: ("https://insights.ins-claims-pinned.us-east-2.indico-dev.indico.io/", "ben.francis@indico.io"),
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



def get_settings(host_option: int) -> Settings:
    try:
        workspace_host, workspace_email = HOST_CHOICES[host_option]
    except KeyError as exc:
        raise ValueError(f"Invalid host option: {host_option}. Valid options: {sorted(HOST_CHOICES)}") from exc

    workspace_password = os.getenv("WORKSPACE_PASSWORD")
    github_token = os.getenv("GITHUB_TOKEN")
    google_api_key = os.getenv("G_API")
    swissre_private_key_path = Path(os.getenv("SWISSRE_PRIVATE_KEY_PATH", "private.key"))

    # These are still hard-coded from your original script, but surfaced here so they are easy to move to env later.
    swissre_client_id = os.getenv("SWISSRE_CLIENT_ID", "0oagnqgsr45l8Nmu70i7")
    swissre_token_url = os.getenv(
        "SWISSRE_TOKEN_URL",
        "https://identity.swissre.com/oauth2/ausaz28lw2bd82Jo50i7/v1/token",
    )
    catnet_base_url = os.getenv("CATNET_BASE_URL", "https://catnet.api-np.swissre.com/demo")

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
    )
