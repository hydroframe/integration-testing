"""
Performance test for the scenario to 1 point for 1 water year
using hf_hydrodata.
"""

# pylint: disable=C0301,W1514,R0914

import os
import importlib
import datetime
import time
import pytz
import socket
import hf_hydrodata as hf


def test_scenario(request):
    """
    Test the scenario to get 365 days of 1 point and
    create logging artifact with performance information.
    """

    local_remote = register_email_pin("public")
    (start_time_str, end_time_str) = get_wy_duration(request)
    t0 = time.time()
    _execute_scenario(start_time_str, end_time_str)
    t1 = time.time()
    duration = round(t1 - t0, 2)
    scenario_name = "read_365_days_1_point"
    write_log(scenario_name, request, local_remote, duration)


def register_email_pin(login_type="private"):
    """
    Register the configured email pin if running remote.
    Returns:
         Either "remote" or "local" depending if running remote.
    """

    if login_type == "private":
        test_email = os.getenv("TEST_EMAIL_PRIVATE")
        test_pin = os.getenv("TEST_PIN_PRIVATE")
    else:
        test_email = os.getenv("TEST_EMAIL_PUBLIC")
        test_pin = os.getenv("TEST_PIN_PUBLIC")

    if test_email is not None and test_pin is not None:
        print(f"Executing remotely using {login_type} email pin")
        local_remote = "remote"
        hf.register_api_pin(f"{test_email}", test_pin)
    else:
        local_remote = "local"
        print("Executing locally without an email pin")
    return local_remote


def get_wy_duration(request):
    """
    Compute the start/end time of the water year specified by the request options.
    Returns:
        Tuple with (start_time_str, end_time_str)
    """

    wy = request.config.getoption("--wy")
    wy = int(wy)
    start_time = datetime.datetime.strptime("2000-10-01", "%Y-%m-%d").replace(year=wy)
    end_time = start_time.replace(year=int(wy + 1))
    start_time_str = start_time.strftime("%Y-%m-%d")
    end_time_str = end_time.strftime("%Y-%m-%d")
    return (start_time_str, end_time_str)


def write_log(scenario_name, request, local_remote, duration):
    """Write the log artificate files"""

    cache_state = request.config.getoption("--cache")
    wy = request.config.getoption("--wy")
    cpus = request.config.getoption("--cpus")
    users = request.config.getoption("--users")
    hf_hydrodata_version = importlib.metadata.version("hf_hydrodata")
    subsettools_version = importlib.metadata.version("subsettools")
    comment = request.config.getoption("--comment")
    if local_remote == "remote":
        hydrodata_url = os.getenv("hydrodata_url", "https://hydrogen.princeton.edu")
        hydrodata_url = hydrodata_url.replace("https://", "")
    else:
        hydrodata_url = ""
    hostname = socket.gethostname()
    log_directory = "./artifacts"
    os.makedirs(log_directory, exist_ok=True)
    est = pytz.timezone('US/Eastern')
    current_time_est = datetime.datetime.now(est)
    cur_date = current_time_est.strftime("%Y-%m-%d:%H:%M:%S")
    line = f"{cur_date},{scenario_name},{hf_hydrodata_version},{hydrodata_url},{subsettools_version},{local_remote},{hostname},{cpus},{users},{cache_state},{wy},{comment},{duration}\n"
    log_file = f"{log_directory}/log_artifact.csv"
    with open(log_file, "a+") as stream:
        stream.write(line)
    print(f"Wrote {log_file}")


def _execute_scenario(start_time_str, end_time_str):
    """Execute the scenario to be tested"""

    bounds = [4057, 1914, 4058, 1915]
    options = {
        "dataset": "CW3E",
        "period": "hourly",
        "dataset_version": "1.0",
        "variable": "precipitation",
        "start_time": start_time_str,
        "end_time": end_time_str,
        "grid_bounds": bounds,
        "nomask": "true",
    }
    data = hf.get_gridded_data(options)
    assert data.shape == (8760, 1, 1)
