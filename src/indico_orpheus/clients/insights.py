from __future__ import annotations

from typing import Any
import httpx


class HTTPException(Exception):
    def __init__(self, error: str, status_code: int):
        self.status_code = status_code
        self.error = error
        super().__init__(f"status code {status_code}: {error}")


class GraphQLError(Exception):
    def __init__(self, error: str):
        self.error = error
        super().__init__(error)


class AuthenticationError(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


class AsyncInsightsClient(httpx.AsyncClient):
    def __init__(
        self,
        host: str,
        email: str,
        password: str,
        follow_redirects: bool = True,
    ):
        if not host.startswith("http://") and not host.startswith("https://"):
            host = f"https://{host}"

        super().__init__(
            base_url=host,
            timeout=None,
            verify=False,
            follow_redirects=follow_redirects,
        )
        self.email = email
        self.password = password

    async def authenticate(self, retries: int = 3) -> None:
        if retries <= 0:
            raise AuthenticationError("Failed to authenticate user")

        response = await super().request(
            "POST",
            "/auth/users/authenticate",
            json={
                "email": self.email,
                "password": self.password,
            },
        )

        if response.status_code in (400, 401):
            raise AuthenticationError(response.text)

    async def request(self, *args: Any, **kwargs: Any) -> httpx.Response:
        resp = await super().request(*args, **kwargs)

        if resp.status_code == 401:
            await self.authenticate()
            return await super().request(*args, **kwargs)

        return resp

    async def call_gql(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> Any:
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        resp = await self.post("/graphql", json=payload)

        if resp.status_code >= 400:
            raise HTTPException(error=resp.text, status_code=resp.status_code)

        resp_json: dict[str, Any] = resp.json()
        if errors := resp_json.get("errors"):
            raise GraphQLError(errors[0]["message"])

        return resp_json["data"]
