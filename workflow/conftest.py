"""Common pytest fixtures."""
import os
import pytest
from parflow.tools.fs import mkdir, rm

@pytest.fixture(scope="session")
def set_parflow_dir():
    """Set ParFlow directory environment variable."""
    old_parflow_dir = os.environ.get("PARFLOW_DIR")
    if not old_parflow_dir:
        # set PARFLOW_DIR to local installation where the tests will be run
        os.environ["PARFLOW_DIR"] = "/home/SHARED/software/parflow/3.10.0"
    yield None
    if old_parflow_dir:
        os.environ["PARFLOW_DIR"] = old_parflow_dir
    else:
        del os.environ["PARFLOW_DIR"]


@pytest.fixture(scope="module")
def setup_dir_structure(set_parflow_dir):
    """Create and return paths for directories necessary for ParFlow run."""
    home = os.path.expanduser("~")
    base_dir = os.path.join(home, "test_output")
    input_dir = os.path.join(base_dir, "inputs")
    output_dir = os.path.join(base_dir, "outputs")

    def setup(run_name):
        static_write_dir = os.path.join(input_dir, run_name, "static")
        mkdir(static_write_dir)
        forcing_dir = os.path.join(input_dir, run_name, "forcing")
        mkdir(forcing_dir)
        pf_out_dir = os.path.join(output_dir, run_name)
        mkdir(pf_out_dir)
        correct_output_dir = os.path.abspath(os.path.join("workflow", "correct_output", run_name))
        target_runscript = os.path.join(pf_out_dir, run_name + ".yaml")
        return (
            static_write_dir,
            forcing_dir,
            pf_out_dir,
            correct_output_dir,
            target_runscript,
        )

    yield setup
    rm(base_dir)
