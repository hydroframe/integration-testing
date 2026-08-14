"""
Performance test for calling subsettools function subset_forcing.
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

    local_remote = utils.register_email_pin("private")
    wy = request.config.getoption("--wy")
    t0 = time.time()
    _execute_scenario(wy)
    t1 = time.time()
    duration = round(t1 - t0, 2)
    scenario_name = "subset_forcing_large"
    utils.write_log(scenario_name, request, local_remote, duration)
    assert os.path.exists("forcing_files/CW3E.Press.000001_to_000024.pfb")
    shutil.rmtree("./forcing_files")
    assert not os.path.exists("forcing_files/CW3E.Press.000001_to_000024.pfb")


def _execute_scenario(wy):
    """Execute the scenario to be tested"""

    wy1 = int(wy) + 1
    os.makedirs("./forcing_files", exist_ok=True)
    forcing_paths = st.subset_forcing(
        ij_bounds=(3874, 1774, 4079, 2180),
        grid="conus2",
        start=f"{wy}-10-01",
        end=f"{wy1}-01-02",
        dataset="CW3E",
        write_dir="./forcing_files",
    )
    assert len(forcing_paths.keys()) == 8
