"""
Performance test for the scenario to 1 point for 1 hour of CONUS2
using hf_hydrodata.
"""

# pylint: disable=C0301,W1514,R0914

import time
import hf_hydrodata as hf
import utils


def test_scenario(request):
    """
    Test the scenario to get 1 hour of 1 point and
    create logging artifact with performance information.
    """

    local_remote = utils.register_email_pin("public")
    (start_time_str, end_time_str) = utils.get_1h_duration(request)

    t0 = time.time()
    _execute_scenario(start_time_str, end_time_str)
    t1 = time.time()
    duration = round(t1 - t0, 2)
    scenario_name = "read_1_hour_1_point"
    utils.write_log(scenario_name, request, local_remote, duration)


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
    assert data.shape == (1, 1, 1)
