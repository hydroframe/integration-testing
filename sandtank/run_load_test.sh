####
# Script to simulate a variable number of users using sandtank in parallel.
# Each call the load_test creates a browser windows and runs a short sandtank
# scenario. Using a larger number can simulate different size load of users
# using sandtank at the same time.
#
# Usage to run 10 users in paralle
#   bash run_load_test.sh 10
#
###

NUM_PROCESSES=${1:-1}
SANDTANK_URL="${2:-https://sandtank.hydroframe.org}"

for i in $(seq 1 "$NUM_PROCESSES"); do
    python load_test.py $i &
done

# After starting the jobs monitor the sandtank
# server to see if it is still responding
python check_sandtank_running.py SANDTANK_URL
