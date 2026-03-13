from __future__ import annotations

from indico_orpheus.clients.insights import AsyncInsightsClient
from indico_orpheus.clients.dnb import DnBClient
from indico_orpheus.config import get_settings
from indico_orpheus.services.workspace_submission import sub_values, push_values
from indico_orpheus.services.json_work import flatten_json

async def run_duns_enrichment(
    submission_id: int,
    workflow_option: int,
    workspace_option: int,
    insured_name_id: str,
    workspace_duns_id: str,
) -> dict:
    settings = get_settings(workflow_option, workspace_option)

    insights_client = AsyncInsightsClient(
        host=settings.workspace_host,
        email=settings.workspace_email,
        password=settings.workspace_password,
    )

    await insights_client.authenticate()

    dnb_client: DnBClient = DnBClient(
        client_id=settings.dnb_client_id,
        client_secret=settings.dnb_client_secret,
        token_url=settings.dnb_token_url,
        base_url=settings.dnb_base_url,
    )

    dnb_client.get_token()

    try:
        insured_name = await sub_values(insights_client, submission_id, insured_name_id)

        result = dnb_client.cleanse_match(insured_name.loc[0])
        flat = flatten_json(result)
        duns = flat.get("matchCandidates[0].organization.duns")

        sanctions = dnb_client.get_sanctions(duns,"NoMonitoring")

        await push_values(insights_client, submission_id, workspace_duns_id, duns)
        return duns
    finally:
        await insights_client.aclose()