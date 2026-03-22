#
# This runs the entire test suite that is normally executed weekly
# in github actions. You can use this script to run it locally.
# Usage:
#.   bash run_suite.sh
# This appends statistics to the csv file in"
#.  artifacts/log_artifacts.csv
export TEST_EMAIL_PUBLIC=hf.test.public@gmail.com
export TEST_PIN_PUBLIC=5201
export TEST_EMAIL_PRIVATE=hf.test.private@gmail.com
export TEST_PIN_PRIVATE=5201
pytest -s test_1pt_1wy.py --wy=2002 --cache=cold
pytest -s test_1pt_1wy.py --wy=2002 --cache=hot
pytest -s test_full_3d_pfb.py --wy=2003 --wy_month=02 --cache=cold
pytest -s test_full_3d_pfb.py --wy=2003 --wy_month=02 --cache=hot
pytest -s test_subset_forcing.py --wy=2007 --cache=cold
pytest -s test_subset_forcing.py --wy=2007 --cache=hot
pytest -s test_subset_forcing_2mo.py --wy=2007 --cache=cold
pytest -s test_subset_forcing_2mo.py --wy=2007 --cache=hot
pytest -s test_subset_forcing_3mo.py --wy=1983 --cache=cold
pytest -s test_subset_forcing_3mo.py --wy=1983 --cache=hot
pytest -s test_subset_forcing_users.py --wy 2009 --cache=cold --users=3
pytest -s test_subset_forcing_users.py --wy 2009 --cache=hot --users=3
pytest -s test_hydrogen_forcing.py --wy 1985 --cache=cold --users=-4
pytest -s test_hydrogen_forcing.py --wy 1985 --cache=hot --users=-4
pytest -s test_cw3e_subset.py --wy 2019 --cache=cold
pytest -s test_cw3e_subset.py --wy 2019 --cache=hot
pytest -s test_1pt_1h.py --wy=1995 --cache=cold
pytest -s test_1pt_1h.py --wy=1995 --cache=hot      
