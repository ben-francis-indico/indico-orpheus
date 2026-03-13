from __future__ import annotations

import json

from indico_orpheus.config import get_settings
from indico_orpheus.clients.intake import IntakeClient


def main() -> None:
    settings = get_settings(1,2)

    workflow_client = IntakeClient(
        workflow_host=settings.workflow_host,
        workflow_token=settings.workflow_token,
    )

    print("Loaded config OK")
    print(f"Host: {workflow_client.workflow_host}")
    print(f"Token Path: {workflow_client.workflow_token}")

    token = workflow_client.get_client()
    print("Token retrieved OK")

    result = workflow_client.get_version()
    print("Endpoint call OK")
    print(json.dumps(result, indent=2)[:3000])


if __name__ == "__main__":
    main()