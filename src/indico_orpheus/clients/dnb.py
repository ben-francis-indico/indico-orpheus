from __future__ import annotations

from typing import Any, Optional
import base64
import requests


class DnBClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_url: str,
        base_url: str,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self.base_url = base_url.rstrip("/")

    def _basic_auth_header(self) -> str:
        raw = f"{self.client_id}:{self.client_secret}"
        encoded = base64.b64encode(raw.encode("utf-8")).decode("utf-8")
        return f"Basic {encoded}"
        
    def get_token(self) -> str:
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": self._basic_auth_header(),
            "Cache-Control": "no-cache"
        }
        
        response = requests.post(
            self.token_url, 
            headers=headers, 
            data={"grant_type": "client_credentials"},
            timeout=60,
        )
        response.raise_for_status()
        
        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise ValueError(f"No access_token in response: {response.text}")
        return token 

    def cleanse_match(
            self,
            company_name: str,
            country_code: str = "US",
            candidate_maximum_quantity: int = 1,
            extra_params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
            token = self.get_token()
            url = f"{self.base_url}/v1/match/cleanseMatch"
    
            params: dict[str, Any] = {
                "name": company_name,
                "countryISOAlpha2Code": country_code,
                "candidateMaximumQuantity": candidate_maximum_quantity,
            }
            if extra_params:
                params.update(extra_params)
    
            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {token}",
            }
    
            response = requests.get(url, headers=headers, params=params, timeout=60)
            response.raise_for_status()
            return response.json()

    def get_company_report(
            self,
            company_name: str,
            country_code: str = "US",
            candidate_maximum_quantity: int = 1,
            extra_params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        token = self.get_token()
        url = f"{self.base_url}/v1/match/cleanseMatch"

        params: dict[str, Any] = {
            "name": company_name,
            "countryISOAlpha2Code": country_code,
            "candidateMaximumQuantity": candidate_maximum_quantity,
        }
        if extra_params:
            params.update(extra_params)

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }

        response = requests.get(url, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        return response.json()


