import os

import requests
import structlog

from standardize_repo_settings.util.logs import setup_logging
from standardize_repo_settings.util.session import get_session

setup_logging()
logger = structlog.get_logger()


GITHUB_ORG = "reichlab"
RULESET_TO_APPLY = "reichlab_default_branch_protections.json"

# source: https://docs.google.com/spreadsheets/d/1UaVsqGQ2uyI42t8HWTQjt0MthQJ-o4Yom0-Q2ahBnJc/edit?gid=1230520805#gid=1230520805
# (any repo with candidate_for_archive column = TRUE)
ARCHIVE_REPO_LIST = [
    "duck-hub",
    # "ensemble-comparison",
    # "Zoltar-Vizualization",
    # "container-demo-app",
    # "2017-2018-cdc-flu-contest",
    # "2018-2019-cdc-flu-contest",
    # "activemonitr",
    # "adaptively-weighted-ensemble",
    # "ALERT",
    # "annual-predictions-paper",
    # "ardfa",
    # "article-disease-pred-with-kcde",
    # "bayesian_non_parametric",
    # "casebot",
    # "cdcfluforecasts",
    # "cdcfluutils",
    # "cdcForecastUtils",
    # "covid-hosp-forecasts-with-cases",
    # "covid19-ensemble-methods-manuscript",
    # "covid19-forecast-evals",
    # "d3-foresight",
    # "dengue-data-stub",
    # "dengue-ssr-prediction",
    # "dengue-thailand-2014-forecasts",
    # "densitystackr",
    # "diffport",
    # "ensemble-size",
    # "flu-eda",
    # "flusight-csv-tools",
    # "Flusight-forecast-data",
    # "FluSight-package",
    # "flusight-test",
    # "flusurv-forecasts-2020-2021",
    # "forecast-framework-demos",
    # "forecastTools",
    # "foresight-visualization-template",
    # "german-flu-forecasting",
    # "hubEnsembles",
    # "kcde",
    # "ledge",
    # "lssm",
    # "make-example",
    # "mmwr-week",
    # "mvtnorm-mod-kcde",
    # "ncov",
    # "neural-stack",
    # "nuxt-forecast-viz",
    # "online-lag-ensemble",
    # "pdtmvn",
    # "pkr",
    # "proper-scores-comparison",
    # "pylssm",
    # "reviewMID",
    # "shiny-predictions",
    # "ssr-influenza-competition",
    # "style",
    # "tracking-ensemble",
    # "TSIRsim",
    # "xgboost-mod",
    # "xgbstack",
    # "xpull",
]


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

    logger.info("Retrieved repositories", org=org_name, repo_count=len(repos))
    return repos


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
