import os

import requests
import structlog

from reichlab_repo_utils import ARCHIVE_REPO_LIST
from reichlab_repo_utils.util.logs import setup_logging
from reichlab_repo_utils.util.repo import get_all_repos
from reichlab_repo_utils.util.session import get_session

setup_logging()
logger = structlog.get_logger()

GITHUB_ORG = "reichlab"
RULESET_TO_APPLY = "reichlab_default_branch_protections.json"


def archive_repo(org_name: str, session: requests.Session):
    """
    Archive repositories in the organization.

    :param org_name: Name of the GitHub organization
    :param session: Requests session for interacting with the GitHub API
    """

    # Get all repositories in the organization
    repos = get_all_repos(org_name, session)
    repo_updates = {
        "archived": True,
    }

    # Only archive repos that are on our list and are not already archived
    repos_to_update = [repo for repo in repos if (repo["name"] in ARCHIVE_REPO_LIST and repo["archived"] is False)]

    update_count = 0
    for repo in repos_to_update:
        repo_name = repo["name"]
        logger.info(repo_name)
        repo_url = f"https://api.github.com/repos/{org_name}/{repo_name}"

        # Archive the repo
        response = session.patch(repo_url, json=repo_updates)
        if response.ok:
            logger.info(f"Successfully archived {repo_name}")
            update_count += 1
        else:
            logger.error("Failed to update repo", repo=repo_name, response=response.json())

    logger.info("Repository archive complete", count=update_count)


def main():
    org_name = GITHUB_ORG
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        logger.error("GITHUB_TOKEN environment variable is required")
        return

    session = get_session(token)

    archive_repo(org_name, session)


if __name__ == "__main__":
    main()
