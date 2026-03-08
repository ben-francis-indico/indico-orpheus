from __future__ import annotations

import json

from indico_orpheus.config import get_settings
from indico_orpheus.clients.dnb import DnBClient


def main() -> None:
    settings = get_settings(2)

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

    result = client.cleanse_match("Microsoft")
    print("Endpoint call OK")
    print(json.dumps(result, indent=2)[:3000])


if __name__ == "__main__":
    main()
