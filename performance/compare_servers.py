"""
    Read the artifacts/log_artificats.csv log file and compare the performance
    difference between the test suite execution for two different
    servers between two time points.
"""

def main():
    # Main routine to run comparison
    # Edit this file to compare server 1 run to server 2 run
    # Edit the start_time, end_times ranges of the logs for each run 
    server1 = "hydrogen.princeton.edu"
    server2 = "hydro-qa2.princeton.edu"
    start_time_1 = "2026-08-09 18:18:00"
    end_time_1 = "2026-08-09 18:57:00"
    start_time_2 = "2026-08-14 18:11:00"
    end_time_2 = "2026-08-14 18:18:010"

    compare_servers(server1, server2, start_time_1, end_time_1, start_time_2, end_time_2)

def compare_servers(server1, server2, start_time_1, end_time_1, start_time_2, end_time_2):
    log1, scenarios = read_logfile(server1, start_time_1, end_time_1)
    log2, scenarios = read_logfile(server2, start_time_2, end_time_2)
    print("COLD DIFFERENCES")
    for scenario in scenarios:
        server1_cold = find_scenario_duration(log1, scenario, "cold")
        server2_cold = find_scenario_duration(log2, scenario, "cold")
        percent = round(server2_cold/server1_cold, 2)
        print(f"  {server1}={server1_cold} {server2}={server2_cold} percent={percent} {scenario}")
    print()
    print("HOT DIFFERENCES")
    for scenario in scenarios:
        server1_cold = find_scenario_duration(log1, scenario, "hot")
        server2_cold = find_scenario_duration(log2, scenario, "hot")
        percent = round(server2_cold/server1_cold, 2)
        print(f"  {server1}={server1_cold} {server2}={server2_cold} percent={percent} {scenario}")

def find_scenario_duration(log_entries, scenario, hotcold):
    """Return the duration of the scenario for the hotcold duration from the log_entries"""
    duration = 0
    for entry in log_entries:
        if entry.get("hotcold") == hotcold and entry.get("scenario") == scenario:
            duration = float(entry.get("duration"))
            break
    return duration

    return duration
def read_logfile(server, start_time, end_time):
    log_results = []
    scenarios = []
    log_file_path = "artifacts/log_artifact.csv"
    with open(log_file_path, "r") as fp:
        content = fp.read()
        for line in content.split("\n"):
            columns = line.split(",")
            if len(columns) > 10:
                log_execution_time = columns[0]
                log_execution_time = normalize_time(log_execution_time)
                log_server = columns[3]
                log_scenario = columns[1]
                log_hotcold = columns[9]
                log_duration = columns[12]
                if log_server == server and log_execution_time >= start_time and log_execution_time <= end_time:
                    entry = {"scenario": log_scenario, "hotcold": log_hotcold, "duration": log_duration}
                    log_results.append(entry)
                    if log_scenario not in scenarios:
                        scenarios.append(log_scenario)
    return log_results, scenarios

def normalize_time(time_str):
    """Normalize time string from "2026-08-07:04:33:40" to 2026-08-07 04:33:40"""
    parts = time_str.split(":")
    if len(parts) == 4:
        time_parts = []
        time_parts.append(parts[1])
        time_parts.append(parts[2])
        time_parts.append(parts[3])
        time_str = parts[0] + " " + ":".join(time_parts)
    return time_str
main()