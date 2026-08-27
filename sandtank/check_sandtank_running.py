"""
Utility to poll every 2 seconds to see if the sandtank server is still working
"""
import sys
import time
import subprocess

MAX_TIME = 10
POLLING_COUNT = 15
POLLING_WAIT = 2

def main():
    sandtank_url = sys.argv[1] if len(sys.argv) > 1 else "https://sandtank.hydroframe.org"

    for i in range(0, POLLING_COUNT):
        print(f"Check {i+1} if sandtank is still running")
        result = is_sandtank_running(sandtank_url)
        if result:
            print(result)
            break
        time.sleep(POLLING_WAIT)
    
def is_sandtank_running(sandtank_url)->str:
    result = None
    start_time = time.time()
    command_result = execute_command(f"curl {sandtank_url}")
    if not "sandtank doesn't work properly without JavaScript enabled" in command_result:
        result = "Sandtank is not working"
    else:
        duration = round(time.time() - start_time,2)
        print(f"   Check for sandtank server still responding in {duration} seconds.")
        if duration > MAX_TIME:
            print("   Sandtank is not responding as fast as it should, but it responded.")
    return result


def execute_command(command:str)->str:
    """Execute shell command and return the stdout of executing the command."""
    parts = command.split(" ")
    process = subprocess.run(parts, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = process.stdout.decode("utf-8")
    return result

if __name__ == "__main__":
    main()
