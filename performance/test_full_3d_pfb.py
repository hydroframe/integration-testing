"""
Performance test for the scenario to 1 full conus2 sized pfb file
using hf_hydrodata.
"""

# pylint: disable=C0301,W1514

import time
import datetime
import hf_hydrodata as hf
import utils


def test_scenario(request):
    """
    Test the scenario to read a full sized conus2 sized pfb.
    and create logging artifact with performance information.
    """

    local_remote = utils.register_email_pin("private")
    (start_time_str, end_time_str) = get_time_range(request)
    t0 = time.time()
    _execute_scenario(start_time_str, end_time_str)
    t1 = time.time()
    duration = round(t1 - t0, 2)
    scenario_name = "read_full_conus2_3d"
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
    wy_month = request.config.getoption("--wy_month")

    start_time = datetime.datetime.strptime(f"{wy}-06-01", "%Y-%m-%d")
    if wy_month:
        start_time = start_time.replace(month=int(wy_month))
    end_time = start_time.replace(hour=1)
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")

    return (start_time_str, end_time_str)


def _execute_scenario(start_time_str, end_time_str):
    """Execute the scenario to be tested"""

    options = {
        "dataset": "conus1_baseline_mod",
        "period": "hourly",
        "variable": "pressure_head",
        "start_time": start_time_str,
        "end_time": end_time_str,
    }

    data = hf.get_gridded_data(options)
    assert data.shape == (1, 5, 1888, 3342)
