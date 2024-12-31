"""
Performance test for the scenario to 1 point for 1 water year
using hf_hydrodata.
"""

#pylint: disable=C0301,W1514

import os
import importlib
import datetime
import time
import hf_hydrodata as hf


def test_scenario(request):
    """
    Test the scenario to get 365 days of 1 point and
    create logging artifcat with performance information.
    """

    wy = request.config.getoption("--wy")
    wy = int(wy)
    start_time = datetime.datetime.strptime("2000-10-01", "%Y-%m-%d").replace(year=wy)
    end_time = start_time.replace(year=int(wy + 1))
    start_time_str = start_time.strftime("%Y-%m-%d")
    end_time_str = end_time.strftime("%Y-%m-%d")
    test_email_private = os.getenv("TEST_EMAIL_PRIVATE")
    test_pin_private = os.getenv("TEST_PIN_PRIVATE")
    print("EMAIL", test_email_private, "PIN", test_pin_private)
    if not test_email_private or not test_pin_private:
        raise ValueError("No email/pin environment variables set")
    hf.register_api_pin(f"{test_email_private}@gmail.com", test_pin_private)
    t0 = time.time()
    _execute_scenario(start_time_str, end_time_str)
    t1 = time.time()
    duration = round(t1 - t0, 2)
    _write_log(request, duration)


def _write_log(request, duration):
    """Write the log artificate files"""

    scenario_name = "read_365_days_1_point"
    cache_state = request.config.getoption("--cache")
    hf_hydrodata_version = importlib.metadata.version("hf_hydrodata")
    comment = request.config.getoption("--comment")
    server = request.config.getoption("--server")
    hydrodata_url = os.getenv("hydrodata_url", "https://hydrogen.princeton.edu")
    hydrodata_url = hydrodata_url.replace("https://", "")
    if server != "remote":
        hydrodata_url = ""

    log_directory = "./artifacts"
    os.makedirs(log_directory, exist_ok=True)
    cur_date = datetime.datetime.now().strftime("%Y-%m-%d:%H:%M:%S")
    line = f"{cur_date},{scenario_name},{hf_hydrodata_version},{hydrodata_url},{server},{cache_state},{comment},{duration}\n"
    with open(f"{log_directory}/log_artifact.csv", "a+") as stream:
        stream.write(line)


def _execute_scenario(start_time_str, end_time_str):
    """Execute the scenario to be tested"""

    bounds = [4057, 1914, 4058, 1915]
    options = {
        "dataset": "CW3E",
        "period": "hourly",
        "variable": "precipitation",
        "start_time": start_time_str,
        "end_time": end_time_str,
        "grid_bounds": bounds,
        "nomask": "true",
    }
    data = hf.get_gridded_data(options)
    assert data.shape == (8760, 1, 1)
