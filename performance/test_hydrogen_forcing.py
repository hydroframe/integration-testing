"""
Performance test for simulating what hydrogen jobs do when collecting scenario
information for a forecast. This collect conus2 dailing forcing variables
of 5 different variables for 90 days of forecast estimation.

This test support the users parameter so you can run the test with x number of
threads doing this test at the same time. If you pass threads=4 this simulates the
four ensemables created by hydrogen jobs.
"""

# pylint: disable=C0301,W1514

import time
import datetime
import concurrent.futures
import hf_hydrodata as hf
import utils


def test_scenario(request):
    """
    Test to use hf_hydrodata to download 5 variables from conus2 daily forcing files
    for 90 days. This is 450 files. If run with users=4 this is is 1800 files as
    are downloaded in a hydrogen forecast scenario. Subset the download with a HUC 8.
    """

    local_remote = utils.register_email_pin("private")
    wy = request.config.getoption("--wy")
    wy = int(wy)
    nthreads = int(request.config.getoption("--users"))
    t0 = time.time()
    if nthreads < 0:
        # Interpret negative number as to run in serial and not in parallel theads
        for index in range(0, -nthreads):
            _execute_scenario(wy, index)
    else:
        # Run nthreads in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for index in range(0, nthreads):
                future = executor.submit(_execute_scenario, wy, index)
                futures.append(future)
            _ = [future.result() for future in concurrent.futures.as_completed(futures)]
    t1 = time.time()
    duration = round(t1 - t0, 2)
    scenario_name = "hydrogen_forcing"
    utils.write_log(scenario_name, request, local_remote, duration)


def _execute_scenario(wy, index):
    """Execute the scenario to be tested"""

    wy = int(wy) + index
    print(f"Read hydrogen forcing WY {wy}")
    start_time_str = f"{wy}-01-01"
    start_time = datetime.datetime.strptime(start_time_str, "%Y-%m-%d")
    start_time_str = start_time.strftime("%Y-%m-%d")
    end_time = start_time + datetime.timedelta(days=90)
    end_time_str = end_time.strftime("%Y-%m-%d")
    for variable in ["downward_shortwave", "precipitation"]:
        options = {
            "dataset": "CW3E",
            "temporal_resolution": "daily",
            "variable": variable,
            "start_time": start_time_str,
            "end_time": end_time_str,
            "huc_id": "10190004",
        }
        data = hf.get_gridded_data(options)
    for aggregation in ["min", "max", "mean"]:
        options = {
            "dataset": "CW3E",
            "temporal_resolution": "daily",
            "variable": "air_temp",
            "aggregation": aggregation,
            "start_time": start_time_str,
            "end_time": end_time_str,
            "huc_id": "10190004",
        }
        data = hf.get_gridded_data(options)
        assert data.shape == (90, 29, 82)
