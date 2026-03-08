from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import jwt
import requests


class SwissReAuthError(Exception):
    pass


class SwissReClient:
    def __init__(
        self,
        client_id: str,
        token_url: str,
        catnet_base_url: str,
        private_key_path: Path,
    ):
        self.client_id = client_id
        self.token_url = token_url
        self.catnet_base_url = catnet_base_url.rstrip("/")
        self.private_key_path = private_key_path

    def _read_private_key(self) -> str:
        return self.private_key_path.read_text(encoding="utf-8")

    def build_client_assertion(self, expires_in_minutes: int = 60) -> str:
        expiry = int((dt.datetime.now() + dt.timedelta(minutes=expires_in_minutes)).timestamp())
        payload = {
            "iss": self.client_id,
            "sub": self.client_id,
            "aud": self.token_url,
            "exp": expiry,
        }
        return jwt.encode(payload, self._read_private_key(), algorithm="RS256")

    def get_access_token(self) -> str:
        client_assertion = self.build_client_assertion()
        response = requests.post(
            self.token_url,
            data={
                "grant_type": "client_credentials",
                "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                "client_assertion": client_assertion,
            },
            timeout=60,
        )
        response.raise_for_status()
        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise SwissReAuthError(f"No access_token in response: {payload}")
        return token

    def post_batch_analysis(
        self,
        body: list[dict[str, Any]],
        version: str = "v2",
        timeout: int = 60,
    ) -> Any:
        token = self.get_access_token()
        url = f"{self.catnet_base_url}/layersAnalysis/batchAnalysis"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        response = requests.post(
            url,
            headers=headers,
            params={"version": version},
            json=body,
            timeout=timeout,
        return response.json()
