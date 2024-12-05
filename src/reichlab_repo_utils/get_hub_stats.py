"""Get row counts for hub model-output and target-data files.

This script loops through a list of GitHub-based Hubverse repositories and counts the
number of lines/rows for each model-output and target data file. It saves the each
hub's information in data/hub_stats directory as a parquet file in the form
{hub_name}.parquet.

Once it has retrieved row counts for all hubs on the list, the script creates
a .csv file with the combined data in the hub_stats directory.
The .csv is recreated each time the script is run and will include data from
all .parquet files in hub_stats (in other words, data from prior script runs
will be included).

Notes
-----
To run this script, you will need a personal GitHub token with read access
to public repositories. The token should be stored in an environment variable
named GITHUB_TOKEN.

The script makes several assumptions:

1. Hub repositories are public and hosted on GitHub
2. All hubs use a directory named "model-output" for model output files
3. Hubs with target data use a directory named "target-data"
4. Files are either CSV or parquet format
5. Parquet files have a .parquet extension
6. No two hubs have the same repository name
7. Users have a machine with a reasonable amount of memory (for expediency, the
script pulls entire .csv files into memory to count the rows instead of chunking
them)

Example
-------

To use this script:

1. Install uv on your machine: https://docs.astral.sh/uv/getting-started/installation/
2. Clone this repo
3. Modify the HUB_REPO_LIST variable in this script. Items should be in the format "owner/repo".
4. From the root of the repo, run the script:
`uv run src/reichlab_repo_utils/get_hub_stats.py`
"""

# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "duckdb",
#   "polars",
#   "requests",
# ]
# ///

import os
from pathlib import Path
from urllib.parse import urlsplit

import duckdb
import polars as pl
import requests

###########################################################
# List of Hubverse repos we're collecting stats for       #
# (commented-out hubs have already been counted & saved)  #
###########################################################
HUB_REPO_LIST: list[str] = [
    # "cdcepi/FluSight-forecast-hub",
    # "reichlab/variant-nowcast-hub",
    # "hubverse-org/flusight_hub_archive",
    # "cdphmodeling/wnvca-2024",
    # "european-modelling-hubs/flu-forecast-hub",
    # "european-modelling-hubs/ari-forecast-hub",
    # "european-modelling-hubs/RespiCompass",
]
###########################################################
# A bunch of init stuff lazily stored in global variables #
###########################################################
try:
    token = os.environ["GITHUB_TOKEN"]
except KeyError:
    raise ValueError("GITHUB_TOKEN environment variable is required")
session = requests.Session()
session.headers.update({"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"})
# For testing, limit number of files to process. Set to 0 for no limit
FILE_COUNT = 0


###########################################################
# Actual work starts here                                 #
###########################################################
def main(owner: str, repo: str, data_dir: str) -> pl.DataFrame:
    output_dir = Path(data_dir)
    output_dir.mkdir(exist_ok=True)

    # duckdb is a handy way to access parquet file metadata
    # (we're not using it to store any output data)
    with duckdb.connect() as con:
        con.sql("INSTALL httpfs;")
        con.sql("SET http_keep_alive=false;")

        repo_line_counts = pl.DataFrame()
        for directory in ["model-output", "target-data"]:
            files = list_files_in_directory(owner, repo, directory)
            if len(files) == 0:
                continue

            count_df = count_rows(con, files)
            if len(count_df) > 0:
                count_df = count_df.with_columns(
                    pl.lit(directory).alias("dir"),
                    pl.lit(f"{owner}/{repo}").alias("repo"),
                )
                # extract team name from file name (it's actually the model_id, but renaming
                # it now would require reprocessing a bunch of data)
                count_df = count_df.with_columns(
                    pl.when(pl.col("dir") == "model-output").then(
                        pl.col("file")
                        .str.slice(11)
                        .str.splitn(".", 2)
                        .struct.rename_fields(["team", "file_type"])
                        .struct.field("team")
                    )
                )
                repo_line_counts = pl.concat([repo_line_counts, count_df])

    repo_line_counts.write_parquet(output_dir / f"{repo}.parquet")
    return repo_line_counts


def count_rows(con, files) -> pl.DataFrame:
    """Returns a dataframe with a line count for each file a list."""
    file_counter = 0
    line_counts = {}

    for file_url in files:
        if FILE_COUNT > 0 and file_counter > FILE_COUNT:
            # if we've set FILE_COUNT for testing and have
            # exceeded it, stop processing files
            break
        file_path = Path(urlsplit(file_url).path)
        file_name = file_path.name
        file_type = file_path.suffix

        if file_type not in [".parquet", ".csv"]:
            continue

        try:
            if file_type == ".csv":
                count = count_rows_csv(file_url)
            else:
                count = count_rows_parquet(con, file_url)
        except Exception:
            print(f"Error processing {file_url}")
            count = None
        finally:
            file_counter += 1
            line_counts[file_name] = count

    count_df = pl.DataFrame({"file": list(line_counts.keys()), "row_count": list(line_counts.values())})

    return count_df


def count_rows_parquet(con, file_url: str) -> int:
    """Use duckdb to get row count from parquet file metadata."""
    sql = f"SELECT num_rows FROM parquet_file_metadata('{file_url}');"
    count = con.sql(sql).fetchone()[0]

    return count


def count_rows_csv(file_url: str) -> int:
    """Get .csv row count by requesting the file and counting lines."""
    response = session.get(file_url)
    response.raise_for_status()
    count = len(response.text.splitlines())

    return count


def list_files_in_directory(owner, repo, directory) -> list[str]:
    """Use GitHub API to get a list of files in a Hub's directory."""
    url: str | None = f"https://api.github.com/repos/{owner}/{repo}/contents/{directory}"

    files = []

    while url:
        response = session.get(url)
        # some hubs don't have a target-data directory, so 404 is a-ok
        if response.status_code == 404:
            print(f"URL {url} not found")
            break
        response.raise_for_status()
        data = response.json()

        for item in data:
            if item["type"] == "file":
                files.append(item["download_url"])
            elif item["type"] == "dir":
                files.extend(list_files_in_directory(owner, repo, item["path"]))

        # Check if there is a next page
        if "next" in response.links:
            url = response.links["next"]["url"]
        else:
            url = None

    print(f"found {len(files)} files in {repo}/{directory}")
    return files


def write_csv(output_dir: Path) -> Path | None:
    """Write output of all hub stats .parquet files to .csv."""
    parquet_files = f"{str(output_dir)}/*.parquet"
    try:
        hub_stats = pl.read_parquet(parquet_files)
        csv_file = output_dir / "hub_stats.csv"
        hub_stats.write_csv(csv_file)
    except pl.exceptions.ComputeError:
        print(f"Cannot create .csv: no parquet files found in {output_dir}")
        csv_file = None

    return csv_file


if __name__ == "__main__":
    data_dir = Path(__file__).parent / "data" / "hub_stats"
    for repo_name in HUB_REPO_LIST:
        print(f"Getting stats for {repo_name}...")
        owner, repo = repo_name.strip().split("/")
        main(owner, repo, str(data_dir))
    print("Updating .csv to include data for all hubs...")
    csv_file = write_csv(data_dir)
    print(f"Hub stats saved to {csv_file}")