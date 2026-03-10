from __future__ import annotations

import json

from indico_orpheus.config import get_settings
from indico_orpheus.clients.dnb import DnBClient
from indico_orpheus.services.json_work import flatten_json


def main() -> None:
    settings = get_settings(1,2)

    client = DnBClient(
        client_id=settings.dnb_client_id,
        client_secret=settings.dnb_client_secret,
        token_url=settings.dnb_token_url,
        base_url=settings.dnb_base_url,
    )

    print("Loaded config OK")
    print(f"Base URL: {client.base_url}")
    print(f"Token URL: {client.token_url}")

    token = client.get_token()
    print("Token retrieved OK")
    print(f"Token preview: {token[:20]}...")

    result = client.cleanse_match("Indico Data Solutions, Inc.")
    print("Endpoint call OK - Cleanse Match test...")
    print(json.dumps(result, indent=2)[:100])

    result_flat = flatten_json(result)
    duns = result_flat.get("matchCandidates[0].organization.duns")

if __name__ == "__main__":
    main()
