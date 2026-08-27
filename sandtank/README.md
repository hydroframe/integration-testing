# Sandtank Stress Test

This directory contains scripts to perform a stress test of sandtank.
This simulate a user running sandtank by using a python module
call playwright to simulate a user opening a web browser and clicking.
This can be run in a script to simulate many users running sandtank
at the same time.

Previous experience shows sandtank can support 11 parallel users, but
starts timing out after 12 simulaneous users. 

Of course in a classroom with 20 students they all might have sandtank
open, but as long as only 11 of them are actually executing sandtank
runs at the same time it should still work. It seems like even if
sandtank gets timeouts after 11 it does not go down.

This test was executed in the old sandtank running on poudre and also the
new sandtank running on poudre2 with the same performance and limits.

## Running the Load Test

Use the run_load_test.sh script to run a load test.

This call simulate one parallel user running sandtank using prod sandtank.

```
bash run_load_test.sh
```

This call simulate 5 paralle users running the test version of sandtank using test sandtank.
```
bash run_load_test.sh 5 "https://sandtank-test.hydroframe.org"
```

