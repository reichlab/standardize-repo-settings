import os
from collections import namedtuple
from itertools import zip_longest

import requests
import structlog
from rich.console import Console
from rich.style import Style
from rich.table import Table

from reichlab_repo_utils.util.logs import setup_logging
from reichlab_repo_utils.util.repo import get_all_repos
from reichlab_repo_utils.util.session import get_session

setup_logging()
logger = structlog.get_logger()

GITHUB_ORG = "reichlab"


def list_repos(org_name: str, session: requests.Session):
    """
    Archive repositories in the organization.

    :param org_name: Name of the GitHub organization
    :param session: Requests session for interacting with the GitHub API
    """

    # Settings for the output columns when listing repo information
    output_column_list = ["name", "created_at", "archived", "visibility", "id"]
    output_column_colors = ["green", "magenta", "cyan", "blue", "yellow"]
    OutputColumns = namedtuple(
        "OutputColumns",
        output_column_list,
    )

    # Create the output table and columns
    console = Console()
    table = Table(
        title=f"Repositories in the {org_name} GitHub organization",
    )
    for col, color in zip_longest(output_column_list, output_column_colors, fillvalue="cyan"):
        # add additional attributes, depending on the column
        style_kwargs = {}
        col_kwargs = {}
        if col == "name":
            col_kwargs = {"ratio": 4}
            style_kwargs = {"link": True}

        style = Style(color=color, **style_kwargs)
        table.add_column(col, style=style, **col_kwargs)

    repos = get_all_repos(org_name, session)
    repo_count = len(repos)

    for repo in repos:
        r = OutputColumns(
            name=f"[link={repo.get('html_url')}]{repo.get('name')}[/link]",
            created_at=str(repo.get("created_at", "")),
            archived=str(repo.get("archived", "")),
            visibility=str(repo.get("visibility", "")),
            id=str(repo.get("id", "")),
        )
        try:
            table.add_row(*r)
        except Exception as e:
            logger.error(f"Error adding row for repo {r.name}: {e}")

    logger.info("Repository report complete", count=repo_count)

    console.print(table)


def main():
    org_name = GITHUB_ORG
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        logger.error("GITHUB_TOKEN environment variable is required")
        return

    session = get_session(token)

    list_repos(org_name, session)


if __name__ == "__main__":
    main()
