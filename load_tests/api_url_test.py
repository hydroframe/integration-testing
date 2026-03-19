# pylint: disable=W0718,C0301,R0914

"""
This is a load test for the HydroGEN API server that tests how many parallal requests
can be handled by the server. This has a command line argument for how many parallel requests
to send. It returns a json object with statistics about the results is sending those parallel
requests. 

This sends the requests to the URL specified by the HYDRODATA_URL environment variable
or to https://hydrogen.princeton.edu if the environment variable is not set.

It also supported different scenarios for executing parallel requests. Two scenarios are supported
'api-pins' to call /api_pins to get the jwt_token of an email/pin
'gridded-data' to call /gridded-data to get some precipitation data in a call that takes about 3 seconds.

Example Usage:
    # Run 1 request to call api_pins
    python api_url_test.py

    # Run 10 parallel requests to api_pins
    python api_load_test.py 10

    # Run 10 parallel requests each to execute api_pins and a call to data gridded_data
    python api_load_test.py 10 api-pins gridde-data

Available scenaries are specified the SCENARIOS global variable.
"""


import sys
import os
import time
import json
import datetime
import concurrent.futures
import requests


SCENARIOS = [
    "api-pins",
    "gridded-data",
    "gridded-data-2",
    "gridded-data-5",
    "gridded-data-8",
    "gridded-data-10",
    "gridded-data-15",
]


def main():
    """
    Main function to run the test from the command line.
    Options can be specified in command line. The first argument
      is the number of parallel requests to execute (default 1).
    The remaining arguments are a list of scenarios to execute (default pin).
    A parallel requests is execute for each scenarios for number of parallel requests.
    The output will return load statitics about each scenario separately.
    """
    try:
        nparallel = int(sys.argv[1]) if len(sys.argv) > 1 else 1
        scenarios = [sys.argv[i] for i in range(0, len(sys.argv)) if i > 1]
        scenarios = scenarios if len(scenarios) > 0 else ["api-pins"]
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
    scenarios = scenarios if scenarios is not None else ["api-pins"]
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
                max_bytes_read = max(max_bytes_read, entry.get("butes_read", 0))
        result = {"scenario": scenario}
        result["test_duration"] = test_duration
        result["nparallel"] = len(execution_results[s_index])
        if max_bytes_read > 0:
            result["bytes_read"] = max_bytes_read
        result["max_duration"] = max_duration
        result["min_duration"] = min_duration
        result["mean_duration"] = total_duration / len(execution_results[s_index])
        duration_list.sort()
        result["median_duration"] = duration_list[int(len(duration_list) / 2)]
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


def send_request(calln: int, execution_results: list[dict], scenario):
    """
    Send an URL request to the API server and store into the execution_results
    array calln entry with a dict describing the execution result.
    """
    st_time = time.time()
    try:
        base_url = os.environ.get("HYDRODATA_URL", "https://hydrogen.princeton.edu")
        email = "hf.test.public@gmail.com"
        pin = "5201"
        url = f"{base_url}/api/api_pins?email={email}&pin={pin}"
        response = requests.get(url, timeout=60)
        duration = time.time() - st_time
        if response.status_code == 400:
            response_json = response.json()
            result = {
                "status": "failure",
                "duration": duration,
                "message": response_json.get("message"),
            }
            execution_results[calln] = result
            return
        if response.status_code != 200:
            result = {
                "status": "failure",
                "duration": duration,
                "message": f"Response code {response.status_code}",
            }
            execution_results[calln] = result
            return
        response_json = response.json()
        result = {}
        if response_json.get("email") == email:
            if scenario == "api-pins":
                result = {"status": "success", "duration": duration}
            elif scenario in SCENARIOS:
                jwt_token = response_json.get("jwt_token", None)
                try:
                    bytes_read = get_data_request(jwt_token, scenario)
                    duration = time.time() - st_time
                    result = {
                        "status": "success",
                        "duration": duration,
                        "butes_read": bytes_read,
                    }
                except Exception as se:
                    result = {
                        "status": "failure",
                        "duration": duration,
                        "message": str(se),
                    }
            else:
                raise ValueError(f"{scenario} is not a known scenario")
            execution_results[calln] = result
        else:
            result = {
                "status": "failure",
                "message": "email is not the same.",
                "duration": duration,
            }
            execution_results[calln] = result
    except Exception as e:
        duration = time.time() - st_time
        result = {"status": "failure", "message": str(e), "duration": duration}
        execution_results[calln] = result


def get_data_request(jwt_token, scenario) -> int:
    """
    Make a gridded-data request to get precipitation data that takes 2-3 second to read.
    Args:
        jwt_token:str       Is a jwt token used for authentication obtained from the /api_pins call
    Raises:
        ValueError if any kind of error occurs in the API call.
    Returns:
        The number of bytes returned in the API call.
    """
    headers = {}
    headers["Authorization"] = f"Bearer {jwt_token}"
    base_url = os.environ.get("HYDRODATA_URL", "https://hydrogen.princeton.edu")
    date_start = "2025-01-01"
    bounds = [0, 0, 1000, 1000]
    days = (
        int(scenario[len("gridded-data-") :])
        if len(scenario) > len("gridded-data-")
        else 1
    )
    days = days**2
    date_start_dt = datetime.datetime.strptime("2025-01-01", "%Y-%m-%d")
    date_end = (date_start_dt + datetime.timedelta(days=days)).strftime("%Y-%m-%d")
    url = f"{base_url}/api/gridded-data?dataset=CW3E&temporal_resolution=daily&variable=precipitation&date_start={date_start}&date_end={date_end}&grid_bounds={bounds}"
    response = requests.get(url, timeout=60, headers=headers)
    if response.status_code == 400:
        response_json = response.json()
        raise ValueError(response_json.get("message"))
    if response.status_code != 200:
        raise ValueError(f"Failed with status_code {response.status_code}")
    return len(response.content)


if __name__ == "__main__":
    main()
