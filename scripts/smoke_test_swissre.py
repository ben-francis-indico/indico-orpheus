from __future__ import annotations

import json

from indico_orpheus.config import get_settings
from indico_orpheus.clients.swissre import SwissReClient


def main() -> None:
    settings = get_settings(1,2)


    client = SwissReClient(
        client_id = settings.swissre_client_id,
        token_url = settings.swissre_token_url,
        catnet_base_url = settings.catnet_base_url.rstrip("/"),
        private_key_path = settings.swissre_private_key_path,
    )



    print("Loaded config OK")

    token = client.get_access_token()
    print("Token retrieved OK")
    print(f"Token preview: {token[:20]}...")

    result = client.health()
    print("Endpoint call OK - SwissRe API is...")
    print(json.dumps(result, indent=2)[:3000])


if __name__ == "__main__":
    main()
