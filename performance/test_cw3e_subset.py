"""
Performance test for the scenario to read a small grid of a CW3E file for one water year
using hf_hydrodata.
"""

# pylint: disable=C0301,W1514,R0914

import time
import datetime
import hf_hydrodata as hf
import utils


def test_scenario(request):
    """
    Test the scenario to read a small grid of a CW3E file for one water year.
    """

    local_remote = utils.register_email_pin("public")
    t0 = time.time()
    (start_time_str, end_time_str) = get_time_range(request)
    _execute_scenario(start_time_str, end_time_str)
    t1 = time.time()
    duration = round(t1 - t0, 2)
    scenario_name = "read_cw3e_subset"
    utils.write_log(scenario_name, request, local_remote, duration)


def get_time_range(request):
    """
    Get the start/end time of one hour of data for the water year specified in the configuration options.
    Returns:
        Tuple (start_time_str, end_time_str)
    """
    start_time_str = ""
    end_time_str = ""
    wy = request.config.getoption("--wy")
    wy = int(wy)

    start_time = datetime.datetime.strptime(f"{wy}-10-01", "%Y-%m-%d")
    end_time = start_time + datetime.timedelta(days=365*1)
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")

    return (start_time_str, end_time_str)

def _execute_scenario(start_time_str, end_time_str):
    """Execute the scenario to be tested"""


    bounds = [2908, 1385,  2923,  1404]
    options = {
        "dataset": "CW3E",
        "period": "hourly",
        "dataset_version": "1.0",
        "variable": "downward_shortwave",
        "start_time": start_time_str,
        "end_time": end_time_str,
        "grid_bounds": bounds,
        "nomask": "true",
    }
    data = hf.get_gridded_data(options)
    assert data.shape == (8760, 19, 15)
