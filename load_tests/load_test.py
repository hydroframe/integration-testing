# pylint: disable=W0718,C0301,R0914

"""
This is a load test for the HydroGEN API server that tests how many parallal requests
can be handled by the server. This has a command line argument for how many parallel users
to simulate while executing the specified scenario.

This executes scenarios using hf_hydrodata to the API server defined by the
environment variable HYDRODATA_URL (by default to https://hydrogen.princeton.edu).

This support 3 scenarios:
    * grid_data            - runs get_gridded_data for 1wy of gridded data for a HUC 6
    * point_data           - runs calls site_variables() and point_data() for 1wy for sites in NJ.
    * site_observations    - runs the hackathon testing get both point_data and grid_data for 1wy data.


Example Usage:
    # Run 1 request to call site_observations
    python load_test.py 10 site_observations

Available scenaries are specified the SCENARIOS global variable.
"""


import sys
import os
import time
import json
import datetime
import importlib
import pytz
import socket
import concurrent.futures
import hf_hydrodata as hf


SCENARIOS = ["site_observations", "grid_data", "point_data", "null_test", "wy_sp"]


def main():
    """
    Main function to run the test from the command line.
    Options can be specified in command line. The first argument
      is the number of parallel requests to execute (default 1)
      the second argument is the name of the scenario to execute.
    A parallel request is executed for each scenarios for number of parallel requests.
    The output will return statitics about the execution.
    """
    try:
        test_email = os.getenv("TEST_EMAIL_PUBLIC")
        test_pin = os.getenv("TEST_PIN_PUBLIC")
        if not test_email or not test_pin:
            print("Set the environment variables TEST_EMAIL_PUBLIC and TEST_PIN_PUBLIC")
            return -1
        hf.register_api_pin(test_email, test_pin)
        nparallel = int(sys.argv[1]) if len(sys.argv) > 1 else 1
        scenario = sys.argv[2] if len(sys.argv) > 2 else "site_observations"
        hot_cold = sys.argv[3] if len(sys.argv) > 3 else "hot"
        if hot_cold not in ["hot", "cold"]:
            print("The 3rd argument must be either hot or cold")
            sys.exit(-1)
        print(f"Starting load test of {scenario} with {nparallel} users ({hot_cold})")
        result = run_test(nparallel, scenario, hot_cold)
        print(json.dumps(result, indent=2))
        write_log(scenario, nparallel, result, hot_cold)
    except Exception as e:
        print(e)


def run_test(nparallel, scenario, hot_cold):
    """
    Run the load test for the scenario for nparallel users.
    Args:
        nparallel:int   Number of parallel requests to execute
        scenario:str    The name of the scenario to execute.
        hot_cold:str    Either hot or cold. If cold try to change date ranges to force cold.
    Returns:
        A array of dict with statistics about running the tests.
    There is a dict in the return list for each scenario.
    Each dict contains attributes: test_duration, max_duration, min_duration,
    mean_duration, median_duration, number_of_errors, types_of_errors.
    """
    if scenario not in SCENARIOS:
        scenario_list = ", ".join(SCENARIOS)
        raise ValueError(f"Scenario '{scenario}' must be one of {scenario_list}")
    execution_results = [{} for _ in range(nparallel)]
    
    st_time = time.time()
    execute_parallel_calls(nparallel, scenario, execution_results, hot_cold)
    duration = time.time() - st_time
    result = format_results(duration, scenario, execution_results)
    return result


def format_results(
    test_duration: float, scenario: str, execution_results: list[dict]
) -> dict:
    """Format the execution results into a json string to return as the result"""
    max_duration = 0
    min_duration = 10000
    total_duration = 0
    number_of_errors = 0
    duration_list = []
    types_of_errors = []
    max_bytes_read = 0
    for entry in execution_results:
        duration = entry.get("duration")
        max_duration = max(duration, max_duration)
        min_duration = min(duration, min_duration)
        total_duration = total_duration + duration
        duration_list.append(duration)
        if entry.get("status") == "failure":
            number_of_errors = number_of_errors + 1
            msg = entry.get("message", "")
            if msg not in types_of_errors:
                types_of_errors.append(msg)
        else:
            max_bytes_read = max(max_bytes_read, entry.get("bytes_read", 0))
    result = {"scenario": scenario}
    result["test_duration"] = test_duration
    result["nparallel"] = len(execution_results)
    if max_bytes_read > 0:
        result["bytes_read"] = max_bytes_read
    result["max_duration"] = round(max_duration, 3)
    result["min_duration"] = round(min_duration, 3)
    result["mean_duration"] = round(
        total_duration / len(execution_results), 3
    )
    duration_list.sort()
    result["median_duration"] = round(duration_list[int(len(duration_list) / 2)], 3)
    result["number_of_errors"] = number_of_errors
    result["types_of_errors"] = types_of_errors
    result["base_url"] = os.environ.get(
        "HYDRODATA_URL", "https://hydrogen.princeton.edu"
    )
    return result


def execute_parallel_calls(
    nparallel: int, scenario: str, execution_results: list[dict], hot_cold:str
):
    """
    Execute nparallel requests to the API server and collect the results
    into the execution_results list.
    """

    futures = []
    nthreads = nparallel

    # Start parallel threads to execute the scenarios
    with concurrent.futures.ThreadPoolExecutor(max_workers=nthreads) as executor:
        for calln in range(0, nparallel):
            future = executor.submit(
                send_request, calln, execution_results, scenario, hot_cold
            )
    # Wait for all threads to complete
    _ = [future.result() for future in concurrent.futures.as_completed(futures)]


def send_request(calln: int, execution_results: list[dict], scenario: str, hot_cold:str):
    """
    Execute the specified scenario for a thread.
    Parameters:
        calln:              The index number of the thread executing
        execution_results:  A dict to put the results indexed by the calln
        scenario:           The name of the scenario to execute.
        hot_cold:str        Either hot or cold. If cold try to change date ranges to force cold.
    """
    st_time = time.time()
    result = {}
    bytes_read = 0
    try:
        if scenario == "site_observations":
            bytes_read = get_site_observations(hot_cold, calln)
        elif scenario == "grid_data":
            bytes_read = get_grid_data(hot_cold, calln)
        elif scenario == "point_data":
            bytes_read = get_point_data(hot_cold, calln)
        elif scenario == "null_test":
            bytes_read = 0
        elif scenario == "wy_sp":
            bytes_read = get_wy_sp()
        else:
            raise ValueError(f"{scenario} is not a known scenario")
        duration = time.time() - st_time
        result = {
            "status": "success",
            "duration": duration,
            "bytes_read": bytes_read,
        }
    except Exception as se:
        duration = time.time() - st_time
        result = {
            "status": "failure",
            "duration": duration,
            "message": str(se),
        }
    except Exception as e:
        duration = time.time() - st_time
        result = {
            "status": "failure",
            "duration": duration,
            "message": str(e),
        }
    execution_results[calln] = result


def get_conus1_site_map(df):
    "Create a dict map to map site_id to an array of [i,j] for conus1 ij point of site."
    result = {}
    for i, row in df.iterrows():
        site_id = row["site_id"]
        conus1_i = row["conus1_i"]
        conus1_j = row["conus1_j"]
        conus2_i = row["conus2_i"]
        conus2_j = row["conus2_j"]
        if is_nan(conus1_i, conus1_j) and not is_nan(conus2_i, conus2_j):
            # We do not have conus1 i,j, but we do have conus2_ij
            lat, lon = hf.to_latlon("conus2", conus2_i, conus2_j)
            conus1_i, conus1_j = hf.to_ij("conus1", lat, lon)
        result[site_id] = [int(conus1_i), int(conus1_j)]
    return result


def is_nan(value_i, value_j):
    """Return True if the value_1 or value_j is None or nan."""

    return (
        value_i is None
        or str(value_i) == "nan"
        or value_j is None
        or str(value_j) == "nan"
    )

def write_log(scenario_name, nparallel, execution_result, hot_cold="hot"):
    """Write the log artifact files"""

    local_remote = "local" if os.path.exists("/hydrodata") else "remote"
    wy = ""
    cpus = ""
    users = nparallel
    hf_hydrodata_version = importlib.metadata.version("hf_hydrodata")
    subsettools_version = importlib.metadata.version("subsettools")
    num_errors = execution_result.get("number_of_errors", 0)
    duration = execution_result.get("test_duration")
    comment = f"Error for {num_errors} user" if num_errors > 0 else ""
    if local_remote == "remote":
        hydrodata_url = os.getenv("HYDRODATA_URL", "https://hydrogen.princeton.edu")
        hydrodata_url = hydrodata_url.replace("https://", "")
    else:
        hydrodata_url = ""
    hostname = socket.gethostname()
    log_directory = "./artifacts"
    os.makedirs(log_directory, exist_ok=True)
    est = pytz.timezone("US/Eastern")
    current_time_est = datetime.datetime.now(est)
    cur_date = current_time_est.strftime("%Y-%m-%d:%H:%M:%S")
    line = f"{cur_date},{scenario_name},{hf_hydrodata_version},{hydrodata_url},{subsettools_version},{local_remote},{hostname},{cpus},{users},{hot_cold},{wy},{comment},{duration}\n"
    log_file = f"{log_directory}/log_artifact.csv"
    with open(log_file, "a+") as stream:
        stream.write(line)
    print(f"Wrote {log_file}")

def get_site_observations(hot_cold:str, calln:int) -> int:
    """
    This site observation scenario was used in the hackathon meeting
    as a scenario to load test both point and gridded data.

    Get site variables for all points in a HUC 8 and read observations 
    for 1 water year of water table depth from those sites from 
    the conus1_baseline_mod dataset using get_gridded_data().
    Raises:
        ValueError if any kind of error occurs in the API call.
    Returns:
        The number of bytes returned in the API calls.
    """
    date_start = "2003-01-01"
    date_end = "2004-01-01"
    huc_id = "14010001"
    filter_options = {
        "dataset": "usgs_nwis",
        "variable": "streamflow",
        "temporal_resolution": "daily",
        "date_start": date_start,
        "date_end": date_end,
        "grid": "conus2",
        "huc_id": [huc_id],
    }

    # Get site variable data
    df = hf.get_site_variables(filter_options)
    df_bytes = int(df.memory_usage(deep=True).sum())
    bytes_read = df_bytes
    site_ids = df["site_id"].tolist()
    conus1_ij_map = get_conus1_site_map(df)

    # Get site observation values for the site_ids
    filter_options = {
        "dataset": "usgs_nwis",
        "variable": "streamflow",
        "temporal_resolution": "daily",
        "date_start": date_start,
        "aggregation": "mean",
        "date_end": date_end,
        "site_ids": site_ids,
    }

    # Get the point data observations
    df = hf.get_point_data(filter_options)
    df_bytes = int(df.memory_usage(deep=True).sum())
    bytes_read = bytes_read + df_bytes

    for site_id in conus1_ij_map:
        grid_point = conus1_ij_map[site_id]
        filter_options = {
            "dataset": "conus1_baseline_mod",
            "variable": "water_table_depth",
            "temporal_resolution": "daily",
            "date_start": date_start,
            "date_end": date_end,
            "grid_point": grid_point,
        }
        data = hf.get_gridded_data(filter_options)
        raw_bytes = data.tobytes()
        bytes_read = bytes_read + len(raw_bytes)

    bytes_read = bytes_read + df_bytes
    return bytes_read


def get_grid_data(hot_cold:str, calln:int) -> int:
    """
    Read 1 year of gridded_data from a HUC 6 from conus2 precipitation data.
    This reads 131 MB of data which is enough to run in queue.
    Raises:
        ValueError if any kind of error occurs in the API call.
    Returns:
        The number of bytes returned in the API calls.
    """
    if hot_cold == "hot":
        wy = 2003
    else:
        wy = 2004 + calln
    date_start = f"{wy}-01-01"
    date_end = f"{wy+1}-01-01"
    huc_id = "140100"
    filter_options = {
        "dataset": "CW3E",
        "variable": "precipitation",
        "temporal_resolution": "daily",
        "date_start": date_start,
        "date_end": date_end,
        "grid": "conus2",
        "huc_id": huc_id,
    }

    # Get site variable data
    data = hf.get_gridded_data(filter_options)
    size = 1
    for dim in data.shape:
        size = size * dim
    bytes_read = size * 8
    return bytes_read


def get_point_data(hot_cold:str, calln:int) -> int:
    """
    Calls both get_site_variables and get_point_data() using hf_hydrodata
    to get info about all sites in NJ and data for 1wy from those sites.
    This is about 107 sites for 365 days and about 348K of data
    and this likely reads 107 .netcdf files to read the data for the sites.
    Raises:
        ValueError if any kind of error occurs in the API call.
    Returns:
        The estimated # of bytes returned in the API calls.
    """
    if hot_cold == "hot":
        wy = 2002
    else:
        wy = 2003 + calln
    options = {
        "dataset": "usgs_nwis",
        "variable": "streamflow",
        "temporal_resolution": "daily",
        "aggregation": "mean",
        "date_start": f"{wy}-01-01",
        "date_end": f"{wy+1}-01-01",
        "state": "NJ",
    }
    # Read site variables for query
    _ = hf.get_site_variables(options)
    bytes_read = 28000 # (got this from server side logs of actual download)

    # Read point data for query
    _ = hf.get_point_data(options)
    bytes_read = bytes_read + 320000 # (got this from server side logs of actual download)
    return bytes_read

def get_wy_sp():
    options = {"dataset": "CW3E", "variable": "precipitation", "temporal_resolution": "hourly",
                "grid": "conus2", "grid_point": [2191, 2097],
                "start_time": "1988-10-01", "end_time": "1989-10-01"}
    data = hf.get_gridded_data(options)
    print(data.shape)
    return 10

if __name__ == "__main__":
    main()
