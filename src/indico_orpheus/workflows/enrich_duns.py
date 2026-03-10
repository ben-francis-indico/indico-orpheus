from __future__ import annotations

from indico_orpheus.clients.insights import AsyncInsightsClient
from indico_orpheus.clients.dnb import DnBClient
from indico_orpheus.config import get_settings

async def run_duns_enrichment(
    submission_id: int,
    host_option: int,
`
    column_names: list[str],
    table_id: str = "CatNet",
) -> dict:
    settings = get_settings(host_option)

    insights_client = AsyncInsightsClient(
        host=settings.workspace_host,
        email=settings.workspace_email,
        password=settings.workspace_password,
    )

    geocoder = GeocoderService(api_key=settings.google_api_key)
    swissre_client = SwissReClient(
        client_id=settings.swissre_client_id,
        token_url=settings.swissre_token_url,
        catnet_base_url=settings.catnet_base_url,
        private_key_path=settings.swissre_private_key_path,
    )

    await insights_client.authenticate()

    try:
        df_loc = await sub_values(insights_client, submission_id, column_names)
        df_loc[["location_latitude", "location_longitude", "location_full_address"]] = df_loc[column_names].apply(
            lambda row: geocoder.geocode_address(", ".join(row.dropna().astype(str))),
            axis=1,
        )

        layers = await get_required_layers(insights_client, submission_id)
        wide = catnet_batch_analysis_df_wide(
            df_loc[["location_latitude", "location_longitude", "location_full_address"]],
            layers,
            swissre_client=swissre_client,
            lat_col="location_latitude",
            lon_col="location_longitude",
        )

        result = df_to_graphql_variables(wide, submission_id=str(submission_id), table_id=table_id)
        await push_enrich(insights_client, result)
        return result
    finally:
        await insights_client.aclose()