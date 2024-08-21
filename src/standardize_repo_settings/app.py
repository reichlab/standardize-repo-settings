import importlib
import json
import os
from pathlib import Path

import requests
import structlog
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry  # type: ignore

from standardize_repo_settings.util.logs import setup_logging

setup_logging()
logger = structlog.get_logger()

# source: https://docs.google.com/spreadsheets/d/1UaVsqGQ2uyI42t8HWTQjt0MthQJ-o4Yom0-Q2ahBnJc/edit?gid=1230520805#gid=1230520805
# (any repo with a WILL_BREAK column = FALSE)
REPO_LIST = [
    "reichlab-python-template",
    # "container-utils",
    # "covidData",
    # "distfromq",
    # "docs.zoltardata",
    # "ensemble-comparison",
    # "flu-hosp-models-2021-2022",
    # "flusion",
    # "forecast-repository",
    # "gbq_operational",
    # "genomicdata",
    # "hub-infrastructure-experiments",
    # "idforecastutils",
    # "jacques",
    # "jacques-covid",
    # "llmtime",
    # "malaria-serology",
    # "predictability",
    # "predtimechart",
    # "qenspy",
    # "qensr",
    # "rclp",
    # "sarimaTD",
    # "sarix-covid",
    # "simplets",
    # "timeseriesutils",
    # "variant-nowcast-hub",
    # "Zoltar-Vizualization",
    # "zoltpy",
    # "zoltr",
]


def get_session(token: str) -> requests.Session:
    """Return a requests session with retry logic."""

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    session = requests.Session()

    # attach a urllib3 retry adapter to the requests session
    # https://urllib3.readthedocs.io/en/latest/reference/urllib3.util.html#urllib3.util.retry.Retry
    retries = Retry(
        total=5,
        allowed_methods=frozenset(["GET", "POST"]),
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update(headers)

    return session


def load_branch_ruleset(filepath: str) -> dict:
    """
    Load branch ruleset from a JSON file.

    :param filepath: Path to the JSON file containing the branch ruleset
    :return: Dictionary containing the branch ruleset
    """
    with open(filepath, "r") as file:
        return json.load(file)


def get_all_repos(org_name: str, session: requests.Session) -> list[dict]:
    """
    Retrieve all repositories from a GitHub organization, handling pagination.

    :param org_name: Name of the GitHub organization
    :param session: Requests session for interacting with the GitHub API
    :return: List of repositories
    """
    repos = []
    repos_url = f"https://api.github.com/orgs/{org_name}/repos"
    while repos_url:
        response = session.get(repos_url)
        response.raise_for_status()
        repos.extend(response.json())
        repos_url = response.links.get("next", {}).get("url")
    return repos


def apply_branch_ruleset(org_name: str, branch_ruleset: dict, session: requests.Session):
    """
    Apply a branch ruleset to every repository in a GitHub organization.

    :param org_name: Name of the GitHub organization
    :param branch_ruleset: Dictionary containing the branch ruleset
    :param session: Requests session for interacting with the GitHub API
    """

    # Get all repositories in the organization
    repos = get_all_repos(org_name, session)

    for repo in repos:
        repo_name = repo["name"]
        logger.info(repo_name)
        if repo_name in REPO_LIST:
            branch_protection_url = f"https://api.github.com/repos/{org_name}/{repo_name}/rulesets"

            # Apply the branch ruleset
            response = session.post(branch_protection_url, json=branch_ruleset)
            if response.ok:
                logger.info(f"Successfully applied branch ruleset to {repo_name}")
            elif response.status_code == 422:
                logger.warning(
                    "Failed to apply branch ruleset (likely because it already exists)",
                    repo=repo_name,
                    response=response.json(),
                )
            else:
                logger.error("Failed to apply branch ruleset", repo=repo_name, response=response.json())


def main():
    org_name = "reichlab"
    token = os.getenv("GITHUB_TOKEN")

    session = get_session(token)

    mod_path = Path(importlib.util.find_spec("standardize_repo_settings").origin).parent
    ruleset_path = mod_path / "rulesets" / "reichlab_default_branch_protections.json"
    branch_ruleset = load_branch_ruleset(str(ruleset_path))

    apply_branch_ruleset(org_name, branch_ruleset, session)


if __name__ == "__main__":
    main()
