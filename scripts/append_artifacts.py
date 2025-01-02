"""
    Append the latest performance log_artifact.csv file to the archive file.
    Call this from a Jenkins job on Verde that run performance tests.
    This appends the laest log_artifact.csv performance metrics to an achive file.
    This combines the metrics computed from remote GitHub action runs from local Jenkins runs.
"""

# pylint: disable=C0301,W1514

import sys
import os


def main():
    """Main routine to download artifacts and append to archive file csv file."""
    if len(sys.argv) <= 1:
        print(
            "Specify log_artifact.csv file and the archive file as command line arguments."
        )
        return
    if len(sys.argv) <= 2:
        print("Specify the archive file as the second command line argument.")
        return
    log_artifact_file = sys.argv[1]
    archive_file = sys.argv[2]
    if not os.path.exists(log_artifact_file):
        print(f"File '{log_artifact_file}' does not exist.")
        return
    with open(log_artifact_file, "r") as fp:
        csv_contents = fp.read()
        append_csv_file(csv_contents, archive_file)


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
