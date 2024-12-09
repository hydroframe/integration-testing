"""
Tests to compare a full ParFlow-CONUS1 subsetting run against
established outputs for headwater watershed(s).
"""
import os
import pytest
import numpy as np
from parflow import Run, read_pfb
from parflow.tools.settings import set_working_directory
import subsettools as st
import hf_hydrodata as hf
from testutils import pf_test_msig_diff

# This is the list of HUCs included in pytest parameterize
HUC_LIST = ["15060201"]

# Number of timesteps (hours) to run simulation for and compare against
NUM_TIMESTEPS = 24

@pytest.fixture(scope="module", autouse=True)
def setup_run(setup_dir_structure, huc_id):
    """
    Fixture to set up and run ParFlow.
    This pytest fixture will be run automatically for use of all 
    tests within this module and setting it up in this way
    ensures it is only run once per module. So all
    tests within this module will use the same ParFlow run,
    without having to run ParFlow multiple times.
    """

    # Define run_name based on grid and HUC provided
    grid = "conus1"
    run_name = f"{grid}_{huc_id}"

    # These could be parameterized if in the future multiple time ranges are desired to be tested
    start = "2002-10-01"
    end = "2002-10-03"


    run_ds = "conus1_baseline_mod"
    var_ds = "conus1_domain"
    forcing_ds = "NLDAS2"

    # Set up directories for ParFlow run
    (
        static_write_dir,
        forcing_dir,
        pf_out_dir,
        correct_output_dir,
        target_runscript,
    ) = setup_dir_structure(run_name)

    # set cluster topology
    P = 1
    Q = 1

    # load template runscript
    reference_run = st.get_template_runscript(
        grid, "transient", "solid", pf_out_dir
    )

    # get ParFlow i/j bounding box
    ij_bounds, mask = st.define_huc_domain(hucs=[huc_id], grid=grid)

    # create mask and solid files
    st.write_mask_solid(mask=mask, grid=grid, write_dir=static_write_dir)

    # subset static inputs and pressure
    st.subset_static(
        ij_bounds,
        dataset=var_ds,
        write_dir=static_write_dir,
        var_list=(
            "slope_x",
            "slope_y",
            "pf_indicator",
            "pme",
            "ss_pressure_head",
        ),
    )
    init_press_filepath = st.subset_press_init(
        ij_bounds,
        dataset=run_ds,
        date=start,
        write_dir=static_write_dir,
        time_zone="UTC",
    )

    # Configure CLM drivers
    st.config_clm(
        ij_bounds, start=start, end=end, dataset=run_ds, write_dir=static_write_dir
    )

    # Subset climate forcing
    st.subset_forcing(
        ij_bounds,
        grid=grid,
        start=start,
        end=end,
        dataset=forcing_ds,
        write_dir=forcing_dir,
    )

    # set up baseline run from template
    target_runscript = st.edit_runscript_for_subset(
        ij_bounds,
        runscript_path=reference_run,
        runname=run_name,
        forcing_dir=forcing_dir,
    )

    # copy static files into run directory
    st.copy_files(read_dir=static_write_dir, write_dir=pf_out_dir)

    # change file names
    target_runscript = st.change_filename_values(
        runscript_path=target_runscript,
        init_press=os.path.basename(init_press_filepath),
    )

    # change processor topology and re-distribute forcing
    target_runscript = st.dist_run(
        topo_p=P,
        topo_q=Q,
        runscript_path=target_runscript,
        dist_clim_forcing=True,
    )

    # Set ParFlow output dir
    set_working_directory(pf_out_dir)

    # Run ParFlow
    run = Run.from_definition(target_runscript)
    run.TimingInfo.StopTime = NUM_TIMESTEPS
    run.run(working_directory=pf_out_dir)

    return (pf_out_dir, run_name, correct_output_dir)

@pytest.mark.parametrize("huc_id", HUC_LIST, scope="module")
def test_parflow_conus1_results(setup_run, huc_id):
    """
    Test to compare run of subsettools workflow against 
    published ParFlow-CONUS1 results.
    """

    # Get run information
    (pf_out_dir, run_name, _) = setup_run

    # Compare to published ParFlow-CONUS simulation results
    conus1_results = hf.get_gridded_data(dataset="conus1_baseline_mod", variable="pressure_head",
                                         huc_id=huc_id,
                                         start_time="2002-10-01", end_time="2002-10-03",
                                         temporal_resolution="hourly")

    # Read in mask. We will mask results at each timestep so the area
    # outside of the  HUC domain is set to NaN
    mask = read_pfb(f"{pf_out_dir}/{run_name}.out.mask.pfb")

    # Test each timestep
    for i in range(NUM_TIMESTEPS):
        timestep = str(i).rjust(5, "0")
        parflow_subset_results = read_pfb(f"{pf_out_dir}/{run_name}.out.press.{timestep}.pfb")
        masked_subset_results = np.where(mask == 0, np.nan, parflow_subset_results)

        print(f">>>>>> checking timestep {i}")

        # ensure the shapes of the arrays returned are the same
        assert masked_subset_results.shape == conus1_results[i, :, :, :].shape

        # The value bounds on these tests are broad and probably specific to the HUC
        # currently chosen. There is an RSE-hydrologist buddy task defined in Notion
        # to investigate the cause of these differences. Once those are determined,
        # this section of the test can be updated to decrease the allowed threshold
        # of difference. Ideally, the test could use pf_test_msig_diff or similar,
        # to test closeness of arrays in the way that the ParFlow testing suite does.
        # The arrays are currently too different to implement that kind of test meaningfully
        # at the moment.
        for j in range(5):
            diff = masked_subset_results[j] - conus1_results[i, j, :, :]
            if j == 0:
                assert (np.nanmax(diff) > -0.6) & (np.nanmax(diff) < 0.75)
            else:
                assert (np.nanmax(diff) > -0.2) & (np.nanmax(diff) < 1.55)


@pytest.mark.parametrize("huc_id", HUC_LIST, scope="module")
def test_parflow_conus1_reproducible(setup_run, huc_id):
    """
    Test to compare run of subsettools workflow against 
    pre-produced and hydrologist-reviewed outputs of a 
    workflow run to ensure the workflow is reproducible.
    """

    # Get run information
    (pf_out_dir, run_name, correct_output_dir) = setup_run

    # Test each timestep
    for i in range(NUM_TIMESTEPS):
        timestep = str(i).rjust(5, "0")
        run_results = read_pfb(f"{pf_out_dir}/{run_name}.out.press.{timestep}.pfb")
        correct_output = read_pfb(f"{correct_output_dir}/{run_name}.out.press.{timestep}.pfb")

        print(f">>>>>> checking timestep {i}")
        assert pf_test_msig_diff(run_results, correct_output, 4)
