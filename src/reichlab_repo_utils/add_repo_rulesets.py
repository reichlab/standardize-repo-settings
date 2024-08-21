import importlib
import json
import os
from pathlib import Path

import requests
import structlog

from reichlab_repo_utils import RULESET_REPO_LIST
from reichlab_repo_utils.util.logs import setup_logging
from reichlab_repo_utils.util.repo import get_all_repos
from reichlab_repo_utils.util.session import get_session

setup_logging()
logger = structlog.get_logger()

GITHUB_ORG = "reichlab"
RULESET_TO_APPLY = "reichlab_default_branch_protections.json"


def load_branch_ruleset(filepath: str) -> dict:
    """
    Load branch ruleset from a JSON file.

    :param filepath: Path to the JSON file containing the branch ruleset
    :return: Dictionary containing the branch ruleset
    """
    with open(filepath, "r") as file:
        return json.load(file)


def apply_branch_ruleset(org_name: str, branch_ruleset: dict, session: requests.Session):
    """
    Apply a branch ruleset to every repository in a GitHub organization.

    :param org_name: Name of the GitHub organization
    :param branch_ruleset: Dictionary containing the branch ruleset
    :param session: Requests session for interacting with the GitHub API
    """

    # Get all repositories in the organization
    repos = get_all_repos(org_name, session)

    # Only update repos that are on our list and are not already archived
    repos_to_update = [repo for repo in repos if (repo["name"] in RULESET_REPO_LIST and repo["archived"] is False)]

    update_count = 0
    for repo in repos_to_update:
        repo_name = repo["name"]
        logger.info(repo_name)
        branch_protection_url = f"https://api.github.com/repos/{org_name}/{repo_name}/rulesets"

        # Apply the branch ruleset
        response = session.post(branch_protection_url, json=branch_ruleset)
        if response.ok:
            logger.info(f"Successfully applied branch ruleset to {repo_name}")
            update_count += 1
        elif response.status_code == 422:
            logger.warning(
                "Failed to apply branch ruleset (likely because it already exists)",
                repo=repo_name,
                response=response.json(),
            )
        else:
            logger.error("Failed to apply branch ruleset", repo=repo_name, response=response.json())

    logger.info("All rulesets applied", count=update_count)


def main():
    org_name = GITHUB_ORG
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        logger.error("GITHUB_TOKEN environment variable is required")
        return

    session = get_session(token)

    mod_path = Path(importlib.util.find_spec("reichlab_repo_utils").origin).parent
    ruleset_path = mod_path / "rulesets" / RULESET_TO_APPLY
    branch_ruleset = load_branch_ruleset(str(ruleset_path))

    apply_branch_ruleset(org_name, branch_ruleset, session)


if __name__ == "__main__":
    main()
