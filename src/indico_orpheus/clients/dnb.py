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

    def get_company_report( #WIP
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

    def get_sanctions( #WIP
            self,
            duns: str,
            screening_monitor: str = "NoMonitoring",
            extra_params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        token = self.get_token()
        url = f"{self.base_url}/v1/screening/inquiries"

        params: dict[str, Any] = {
            "duns": duns,
            "screening_monitor": screening_monitor,
        }
        if extra_params:
            params.update(extra_params)

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }
        url_f="https://plus.dnb.com/v1/screening/inquiries?customerTransactionID=1234&screeningMonitoringMode=DataAndMonitoring&reviewStatus=Reviewed&caseID=8a71d7fe-0847-abc-8fb1-f486a50bdf20&inquiryIDs=8a71d7fe-0847-476a-8fb1-f486a50bdf20&duns=804735132&pageSize=25&pageNumber=2&isInitialInquiry=true&lastUpdatedStartDate=2022-01-01&lastUpdatedEndDate=2022-02-01&startDate=2019-02-01&endDate=2019-03-01"
        #response = requests.get(url, headers=headers, params=params, timeout=60)
        response = requests.get(url_f, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        return response.json()


