"""
Performance test for the scenario to load 1 full CONUS2 sized pfb file using hf_hydrodata.

This file is too big to download remotely in one call to get_gridded_data() because of timeout.
This also does not work when reading 8 parts of the file in parallel because of timeout when cold.
We will later try to increase workers in API server to allow the download to be done in parallel.

The number of threads to use when reading can be specified with the --users command line argument.
If --users is negative it reads the parts in serial, if it is positive it reads in parallel.

This test also support the argument --wy_month to specify which month of the water year to use.
Using this allows you to run the test remote and local with different months to test both cold.
The 3D files for conus are only available in the 2003 water year so you must change the month
to get different data.

For now this test is checked in, but not run by the GitHub actions.
Using 2 or 4 parts in serial when cold seems to work. For example,
   pytest test_conus2_3d_pfb.py --wy 2003 --cache cold --wy_month=2 --users=-4
"""

# pylint: disable=C0301,W1514,W0718,W0101

import time
import datetime
import concurrent.futures
import numpy as np
import hf_hydrodata as hf
import utils


def test_scenario(request):
    """
    Test the scenario to read a full sized conus2 sized pfb.
    and create logging artifact with performance information.
    """

    nthreads = int(request.config.getoption("--users"))
    local_remote = utils.register_email_pin("private")
    (start_time_str, end_time_str) = get_time_range(request)
    t0 = time.time()
    data = _execute_scenario(start_time_str, end_time_str, nthreads)
    t1 = time.time()
    duration = round(t1 - t0, 2)

    # Do an assertion on the sum of the data read to validate correctness
    # There is a different assertion depending on the --wy_month that reads different data
    # This means the remote and local tests can pass different --wy_month to run both cold
    wy_month = int(request.config.getoption("--wy_month"))
    data_sum = np.nansum(data)
    if wy_month == 2:
        assert int(data_sum) == 3577883953
    elif wy_month == 3:
        assert int(data_sum) == 3581459028
    elif wy_month == 4:
        assert int(data_sum) == 3580283541

    scenario_name = "read_conus2_3d"
    utils.write_log(scenario_name, request, local_remote, duration)


def get_time_range(request):
    """
    Get the start/end time of one hour of data for the water year specified in the configuration options.
    Returns:
        Tuple (start_time_str, end_time_str)
    """
    start_time_str = ""
    end_time_str = ""
    wy = int(request.config.getoption("--wy"))
    wy_month = int(request.config.getoption("--wy_month"))

    start_time = datetime.datetime.strptime(f"{wy}-06-01", "%Y-%m-%d")
    if wy_month:
        start_time = start_time.replace(month=int(wy_month))
    end_time = start_time.replace(hour=1)
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")

    return (start_time_str, end_time_str)


def _execute_scenario(start_time_str, end_time_str, nthreads):
    """Execute the scenario to be tested"""

    data = np.zeros((1, 10, 3256, 4442))
    if nthreads < 0:
        # Read the file in nthreads serial calls hf.get_gridded_data()
        _run_serial(data, start_time_str, end_time_str, -nthreads)
    else:
        # Read the file with nthread calls to hf.get_gridded_data in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            num_parts = nthreads
            for part in range(0, num_parts):
                future = executor.submit(
                    _thread_excecutor,
                    data,
                    part,
                    num_parts,
                    start_time_str,
                    end_time_str,
                )
                futures.append(future)
            _ = [future.result() for future in concurrent.futures.as_completed(futures)]

    assert data.shape == (1, 10, 3256, 4442)
    return data


def _thread_excecutor(result, part: int, num_parts: int, start_time_str, end_time_str):
    """Called from thread pool to read one part of the file into the result numpy array."""
    try:
        ng_x = 4442
        ny_y = 3256
        bounds = [
            0,
            part * int(ny_y / num_parts),
            ng_x,
            (part + 1) * int(3256 / num_parts),
        ]

        options = {
            "dataset": "conus2_baseline",
            "period": "hourly",
            "variable": "pressure_head",
            "grid_bounds": bounds,
            "start_time": start_time_str,
            "end_time": end_time_str,
            "npart": part,
        }

        print(f"Read data for part {part} of {num_parts}", options)
        t0 = time.time()
        data = hf.get_gridded_data(options)
        t1 = time.time()
        result[
            :, :, part * int(ny_y / num_parts) : (part + 1) * int(ny_y / num_parts), :
        ] = data
        print(f"Finished read of part {part} in {t1-t0} seconds.")
    except Exception as e:
        t1 = time.time()
        print(f"Failed get_gridded_data part {part}", e, f"duration = {t1-t0}")


def _run_serial(result, start_time_str, end_time_str, nthreads):
    """Called to read nthreads parts of the file in serial into the result numpy array."""

    try:
        num_parts = nthreads
        for part in range(0, num_parts):
            ng_x = 4442
            ny_y = 3256
            bounds = [
                0,
                part * int(ny_y / num_parts),
                ng_x,
                (part + 1) * int(3256 / num_parts),
            ]

            options = {
                "dataset": "conus2_baseline",
                "period": "hourly",
                "variable": "pressure_head",
                "grid_bounds": bounds,
                "start_time": start_time_str,
                "end_time": end_time_str,
                "npart": part,
            }

            print(f"Read data for part {part} of {num_parts}", options)
            t0 = time.time()
            data = hf.get_gridded_data(options)
            t1 = time.time()
            result[
                :,
                :,
                part * int(ny_y / num_parts) : (part + 1) * int(3256 / num_parts),
                :,
            ] = data
            print(f"Finished read of part {part} in {t1-t0} seconds.")
    except Exception as e:
        t1 = time.time()
        print(f"Failed get_gridded_data part {part}", e, f"duration = {t1-t0}")
