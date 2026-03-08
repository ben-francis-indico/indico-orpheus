from __future__ import annotations
from github import Github, Auth

def github_gql_object(repo_name: str, file_path: str, github_token: str, ref: str = "main") -> str:
    client = Github(auth=Auth.Token(github_token))
    repo = client.get_repo(repo_name)
    file_obj = repo.get_contents(file_path, ref=ref)
    return file_obj.decoded_content.decode("utf-8")
