# pylint: disable=W0718,C0301,R0914

"""
This is a load test for the HydroGEN API server that tests how many parallal requests
can be handled by the server. This has a command line argument for how many parallel requests
to send and which scenarios to execute.

This executes scenarios using hf_hydrodata to the API server defined by the
environment variable HYDRODATA_URL, by default to https://hydrogen.princeton.edu.


Example Usage:
    # Run 1 request to call site_observations
    python api_load_test.py 10 site_observations

Available scenaries are specified the SCENARIOS global variable.

"""


import sys
import os
import time
import json
import datetime
import concurrent.futures
import hf_hydrodata as hf


SCENARIOS = [
    "site_observations",
]


def main():
    """
    Main function to run the test from the command line.
    Options can be specified in command line. The first argument
      is the number of parallel requests to execute (default 1).
    The remaining arguments are a list of scenarios to execute/.
    A parallel requests is execute for each scenarios for number of parallel requests.
    The output will return load statitics about each scenario separately.
    """
    try:
        nparallel = int(sys.argv[1]) if len(sys.argv) > 1 else 1
        scenarios = [sys.argv[i] for i in range(0, len(sys.argv)) if i > 1]
        scenarios = scenarios if len(scenarios) > 0 else ["site_observations"]
        result = run_test(nparallel, scenarios)
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(str(e))


def run_test(nparallel, scenarios=None):
    """
    Run the load test.
    Args:
        nparallel:int   Number of parallel requests to execute
        scenarios:list[str]     A list of scenarios to execute in the test
    Returns:
        A array of dict with statistics about running the tests.
    There is a dict in the return list for each scenario.
    Each dict contains attributes: test_duration, max_duration, min_duration,
    mean_duration, median_duration, number_of_errors, types_of_errors.
    """
    scenarios = scenarios if scenarios is not None else ["site_observations"]
    for scenario in scenarios:
        if scenario not in SCENARIOS:
            scenario_list = ", ".join(SCENARIOS)
            raise ValueError(f"Scenario '{scenario}' must be one of {scenario_list}")
    execution_results = [
        [{} for _ in range(nparallel)] for _ in range(0, len(scenarios))
    ]
    st_time = time.time()
    execute_parallel_calls(nparallel, scenarios, execution_results)
    duration = time.time() - st_time
    result = format_results(duration, scenarios, execution_results)
    return result


def format_results(
    test_duration: float, scenarios: list[str], execution_results: list[list[dict]]
) -> dict:
    """Format the execution results into a json string to return as the result"""
    result_list = []
    for s_index, scenario in enumerate(scenarios):
        max_duration = 0
        min_duration = 10000
        total_duration = 0
        number_of_errors = 0
        duration_list = []
        types_of_errors = []
        max_bytes_read = 0
        for entry in execution_results[s_index]:
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
        result["nparallel"] = len(execution_results[s_index])
        if max_bytes_read > 0:
            result["bytes_read"] = max_bytes_read
        result["max_duration"] = round(max_duration, 3)
        result["min_duration"] = round(min_duration, 3)
        result["mean_duration"] = round(total_duration / len(execution_results[s_index]), 3)
        duration_list.sort()
        result["median_duration"] = round(duration_list[int(len(duration_list) / 2)],3)
        result["number_of_errors"] = number_of_errors
        result["types_of_errors"] = types_of_errors
        result["base_url"] = os.environ.get(
            "HYDRODATA_URL", "https://hydrogen.princeton.edu"
        )
        result_list.append(result)
    return result_list


def execute_parallel_calls(
    nparallel: int, scenarios: list[str], execution_results: list[list[dict]]
):
    """
    Execute nparallel requests to the API server and collect the results
    into the execution_results list.
    """

    futures = []
    nthreads = nparallel * len(scenarios)
    with concurrent.futures.ThreadPoolExecutor(max_workers=nthreads) as executor:
        for calln in range(0, nparallel):
            for s_index, scenario in enumerate(scenarios):
                future = executor.submit(
                    send_request, calln, execution_results[s_index], scenario
                )
    _ = [future.result() for future in concurrent.futures.as_completed(futures)]


def send_request(calln: int, execution_results: list[dict], scenario:str):
    """
    Send an URL request to the API server and store into the execution_results
    array calln entry with a dict describing the execution result.
    """
    st_time = time.time()
    result = {}
    if scenario == "site_observations":
        try:
            bytes_read = get_site_observations()
            duration = time.time() - st_time
            result = {
                "status": "success",
                "duration": duration,
                "bytes_read": bytes_read,
            }
        except Exception as se:
            print(se)
            result = {
                "status": "failure",
                "duration": duration,
                "message": str(se),
            }
    else:
        raise ValueError(f"{scenario} is not a known scenario")
    execution_results[calln] = result

def get_site_observations() -> int:
    """
    Get site observations for a huc and read observations for 1 water for those sites.
    Raises:
        ValueError if any kind of error occurs in the API call.
    Returns:
        The number of bytes returned in the API calls.
    """
    date_start = "2003-01-01"
    date_end = "2003-02-01"
    huc_id = "14010002"
    filter_options = {
        "dataset": "usgs_nwis",
        "variable": "streamflow",
        "temporal_resolution": "daily",
        "date_start": date_start,
        "date_end": date_end,
        "grid": "conus2",
        "huc_id": [huc_id]
    }

    # Get site variable data
    df = hf.get_site_variables(filter_options)
    df_bytes = int(df.memory_usage(deep=True).sum())
    bytes_read = df_bytes
    site_ids = df['site_id'].tolist()
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
        #print(site_id, grid_point)
        filter_options = {
            "dataset": "NLDAS2",
            "variable": "atmospheric_pressure",
            "temporal_resolution": "daily",
            "date_start": date_start,
            "date_end": date_end,
            "grid_point": grid_point
        }
        data = hf.get_gridded_data(filter_options)
        #print("Site data", site_id, data.shape)
        raw_bytes = data.tobytes()
        bytes_read = bytes_read + len(raw_bytes)

    bytes_read = bytes_read + df_bytes
    return bytes_read

def get_conus1_site_map(df):
    "Create a dict map to map site_id to an array of [i,j] for conus1 ij point of site."
    result = {}
    for i,row in df.iterrows():
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

    return value_i is None or str(value_i) == "nan" or value_j is None or str(value_j) == "nan"

if __name__ == "__main__":
    main()
