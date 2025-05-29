"""
Performance test for calling subsettools function subset_forcing
to download 2 years of hourly data for 8 variables.

Do no try to run the job every week or try to run this job
remotely since it would run out of disk space.
"""

# pylint: disable=C0301,W1514

import os
import time
import shutil
import subsettools as st
import utils


def test_scenario(request):
    """
    Test the scenario to call subsettools function subset_forcing and log timing.
    This downloads 248 forcing files (1 month, 8 vars) which are removed after the test executes.
    """

    #local_remote = utils.register_email_pin("private")
    wy = request.config.getoption("--wy")
    wy = int(wy)
    data_dir = "./test_dir"
    os.makedirs(data_dir, exist_ok=True)
    # Do not run this normally because it uses too much disk space
    #return
    t0 = time.time()
    _execute_scenario(data_dir, wy)
    t1 = time.time()
    duration = round(t1 - t0, 2)
    scenario_name = "subset_forcing_years"
    print(f"Download forcing for 5 years in {duration} seconds.")
    #utils.write_log(scenario_name, request, local_remote, duration)
    assert os.path.exists(f"{data_dir}/CW3E.Press.000001_to_000024.pfb")
    shutil.rmtree(f"{data_dir}")
    assert not os.path.exists("{data_dir}/CW3E.Press.000001_to_000024.pfb")


def _execute_scenario(data_dir, wy):
    """Execute the scenario to be tested"""

    forcing_paths = st.subset_forcing(
        ij_bounds = (2909, 1385, 2921, 1403),   # Mickey Rush's bounds
        grid="conus2",
        start=f"{wy}-10-01",
        end=f"2022-10-01",
        dataset="CW3E",
        write_dir=data_dir,
    )
    assert len(forcing_paths.keys()) == 8
