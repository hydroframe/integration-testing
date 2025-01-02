"""Download the latest performance artificat from the GitHub actions runs."""

# pylint: disable=C0301,W1514

import sys
import os
import zipfile
import io
import requests


def main():
    """Main routine to download artificats and append to archive file csv file."""
    if len(sys.argv) <= 1:
        print("Specify archive file as command line argument")
        return
    archive_file = sys.argv[1]
    headers = get_url_headers()
    run_id = get_latest_run_id(headers)
    if run_id is None:
        print("No performance runs found")
        return
    csv_contents = get_artifact(headers, run_id)
    append_csv_file(csv_contents, archive_file)


def get_url_headers():
    """Create the headers map to be used for security tokens in URL get requests."""

    github_token = os.getenv("GITHUB_TOKEN")
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github+json",
    }
    return headers


def get_latest_run_id(headers):
    """Get the latest run id of th get hub actions."""

    run_id = None
    url = "https://api.github.com/repos/hydroframe/integration-testing/actions/runs"
    response = requests.get(url, headers=headers, timeout=20)
    response.raise_for_status()
    data = response.json()
    runs = data.get("workflow_runs")
    for run in runs:
        run_name = run.get("name")
        if run_name == "Run Performance Tests":
            run_id = run.get("id")
            break
    return run_id


def get_artifact(headers, run_id):
    """Get the contents of the artifact of the github run_id."""

    csv_contents = ""
    url = f"https://api.github.com/repos/hydroframe/integration-testing/actions/runs/{run_id}/artifacts"
    response = requests.get(url, headers=headers, timeout=20)
    response.raise_for_status()
    run_json = response.json()
    artifacts = run_json.get("artifacts")
    if not artifacts or len(artifacts) == 0:
        print(f"No artificats in latest run {run_id}.")
        return ""
    artifact = artifacts[0]
    url = artifact.get("archive_download_url")
    response = requests.get(url, headers=headers, timeout=20)
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        for file_name in z.namelist():
            if file_name == "log_artifact.csv":
                with z.open(file_name) as f:
                    csv_contents = f.read().decode("utf-8")
    return csv_contents


def append_csv_file(csv_contents, archive_csv_file):
    """Append the rows from csv_contents to the archive file for rows that are not already in the file."""

    date_map = {}
    if os.path.exists(archive_csv_file):
        # Archive file exists, read file to collect all dates in file
        with open(archive_csv_file, "r") as fp:
            file_contents = fp.read()
            for line in file_contents.split("\n"):
                columns = line.split(",")
                date_map[columns[0]] = True
    else:
        # Archive file does not exists, create it with a CSV file header
        with open(archive_csv_file, "a+") as fp:
            fp.write(
                "date,scenario,hf_hydrodata_version,hydrodata_url,subsettools_version,remotelocal,server,cpus,hotcold,wy,comment,duration\n"
            )

    added_rows = 0
    with open(archive_csv_file, "a+") as fp:
        for line in csv_contents.split("\n"):
            if line:
                columns = line.split(",")
                if not date_map.get(columns[0]):
                    fp.write(f"{line}\n")
                    added_rows = added_rows + 1
    print(f"Added {added_rows} rows to {archive_csv_file} file.")


if __name__ == "__main__":
    main()
