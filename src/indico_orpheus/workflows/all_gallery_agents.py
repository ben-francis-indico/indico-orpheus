from __future__ import annotations
import json
from indico_orpheus.config import get_settings
from indico_orpheus.clients.intake import IntakeClient, GraphQLRequest
import pandas as pd

def component_blueprints_to_dataframe(payload: dict) -> pd.DataFrame:
    """
    Convert workflow componentBlueprints JSON into a DataFrame with columns:
    id, name, fields, footer

    Rules:
    - one row per field name
    - id/name/footer repeat for each associated field
    - components with no fields are skipped
    - footer is set to None unless present in the source JSON
    """
    blueprints = payload["data"]["workflow"]["componentBlueprints"]
    rows = []

    for blueprint in blueprints:
        component_id = blueprint.get("id")
        component_name = blueprint.get("name")
        footer = blueprint.get("footer")  # will be None if not present
        fields = blueprint.get("fields", [])

        for field in fields:
            rows.append({
                "id": component_id,
                "name": component_name,
                "fields": field.get("name"),
                "footer": footer,
            })

    return pd.DataFrame(rows, columns=["id", "name", "fields", "footer"])

def get_gallery_agents(workflow_client) -> str:
    client = workflow_client.get_client()
    query = """
    query AllGalleryAgents {
      workflow(id: 4997) {
        componentBlueprints {
          id
          name
          fields {
            name
          }
          footer
        }
      }
    }
    """

    req = GraphQLRequest(
        query=query,
        variables={}
    )

    response: object = client.call(req)

    return response

def main() -> None:
    settings = get_settings(1,2)

    workflow_client = IntakeClient(
        workflow_host=settings.workflow_host,
        workflow_token=settings.workflow_token,
    )

    resp=get_gallery_agents(workflow_client)
    print(resp)
    df = component_blueprints_to_dataframe(resp)
    print(df)
    #write df to Excel

if __name__ == "__main__":
    main()

