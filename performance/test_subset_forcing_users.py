"""
Performance test for calling subsettools function subset_forcing
in parallel threads to simulate multiple users download data at the same time.
"""

# pylint: disable=C0301,W1514

import os
import time
import shutil
import concurrent.futures
import subsettools as st
import utils


def test_scenario(request):
    """
    Test the scenario to call subsettools function subset_forcing and log timing.
    This downloads 248 forcing files (1 month, 8 vars) which are removed after the test executes.
    The scenario is executed in multiple threads to simulate several people downloading at the same time.
    """

    local_remote = utils.register_email_pin("private")
    wy = request.config.getoption("--wy")
    wy = int(wy)
    nthreads = int(request.config.getoption("--users"))
    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for index in range(0, nthreads):
            data_dir = f"./forcing_files_{index}"
            future = executor.submit(_execute_scenario, data_dir, wy, index)
            futures.append(future)
        _ = [future.result() for future in concurrent.futures.as_completed(futures)]
    t1 = time.time()
    duration = round(t1 - t0, 2)
    scenario_name = "subset_forcing_users"
    utils.write_log(scenario_name, request, local_remote, duration)
    for index in range(0, nthreads):
        data_dir = f"./forcing_files_{index}"
        assert os.path.exists(f"{data_dir}/CW3E.Press.000001_to_000024.pfb")
        shutil.rmtree(data_dir)
        assert not os.path.exists(f"{data_dir}/CW3E.Press.000001_to_000024.pfb")


def _execute_scenario(data_dir, wy, index):
    """Execute the scenario to be tested"""

    os.makedirs(data_dir, exist_ok=True)
    start_month = 10 + index if index <= 2 else ((index - 3) % 12) + 1
    start_year = wy if index <= 2 else wy + int((index - 3) / 12) + 1
    end_month = 10 + index + 1 if index <= 1 else ((index - 2) % 12) + 1
    end_year = wy if index <= 1 else wy + int((index - 2) / 12) + 1
    start_str = f"{start_year}-{start_month:02}-01"
    end_str = f"{end_year}-{end_month:02}-01"
    forcing_paths = st.subset_forcing(
        ij_bounds=(2865, 1143, 2923, 1184),
        grid="conus2",
        start=start_str,
        end=end_str,
        dataset="CW3E",
        write_dir=data_dir,
    )
    assert len(forcing_paths.keys()) == 8
