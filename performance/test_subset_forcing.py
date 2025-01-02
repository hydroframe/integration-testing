"""
Performance test for calling subsettools function subset_forcing.
"""

# pylint: disable=C0301,W1514

import os
import time
import shutil
import subsettools as st
import test_1pt_1wy


def test_scenario(request):
    """
    Test the scenario to call subsettools function subset_forcing and log timing.
    This downloads 248 forcing files (1 month, 8 vars) which are removed after the test executes.
    """

    local_remote = test_1pt_1wy.register_email_pin("private")
    t0 = time.time()
    _execute_scenario()
    t1 = time.time()
    duration = round(t1 - t0, 2)
    scenario_name = "subset_forcing"
    test_1pt_1wy.write_log(scenario_name, request, local_remote, duration)
    assert os.path.exists("forcing_files/CW3E.Press.000001_to_000024.pfb")
    shutil.rmtree("./forcing_files")
    assert not os.path.exists("forcing_files/CW3E.Press.000001_to_000024.pfb")



def _execute_scenario():
    """Execute the scenario to be tested"""

    os.makedirs("./forcing_files", exist_ok=True)
    forcing_paths = st.subset_forcing(
        ij_bounds = (2865, 1143, 2923, 1184),
        grid="conus2",
        start = "2007-10-01",
        end="2007-11-01",
        dataset = "CW3E",
        write_dir="./forcing_files")
    assert len(forcing_paths.keys()) == 8
