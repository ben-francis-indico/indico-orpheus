from __future__ import annotations

from pathlib import Path

from indico import IndicoClient, IndicoConfig
from indico.client.request import GraphQLRequest

class IntakeClient:
    def __init__(
        self,
        workflow_host: str,
        workflow_token: str,
    ):
        self.workflow_host = workflow_host
        self.workflow_token = workflow_token

    def get_client(self) -> IndicoClient:
        config = IndicoConfig(
            host=self.workflow_host,
            api_token_path=self.workflow_token,
        )
        return IndicoClient(config=config)

    def get_version(self) -> str:
        client = self.get_client()
        query = """
        query MyQuery {
            ipaVersion
        }
        """

        req = GraphQLRequest(
            query=query,
            variables={}
        )

        response = client.call(req)

        return(response)
