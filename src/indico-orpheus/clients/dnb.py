from __future__ import annotations
import requests

def get_token(encoded_key: str = dnb_key) -> str:
    url = "https://plus.dnb.com/v3/token"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {encoded_key}",
        "Cache-Control": "no-cache"
    }

    payload = {"grant_type": "client_credentials"}

    response = requests.post(url, headers=headers, data=payload)
    response.raise_for_status()

    return response.json().get("access_token")

def cleanse_match(token: str, company_name: str) -> dict:
    url = "https://plus.dnb.com/v1/match/cleanseMatch"

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}"
    }

    params = {"name": company_name, "countryISOAlpha2Code": "US", "candidateMaximumQuantity": 1}

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    return response.json()
