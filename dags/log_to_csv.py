import pandas as pd
from pathlib import Path

# Define directories
BASE_DIR = Path("d:/Documents/UK/University/BI/Repo/airflow-etl/data")
LOG_DIR = BASE_DIR
STAGE_DIR = BASE_DIR / "StagingArea"

def process_logs_to_csv():
    """Process all log files in LOG_DIR and combine them into a single CSV."""
    # Ensure the staging directory exists
    STAGE_DIR.mkdir(parents=True, exist_ok=True)

    combined_data = []
    print(f"Looking for log files in: {LOG_DIR}")
    log_files = list(LOG_DIR.glob('*.log'))
    print(f"Files found: {log_files}")

    for log_file in log_files:
        print(f"Processing file: {log_file}")
        with open(log_file, 'r') as infile:
            for i, line in enumerate(infile):
                if line.startswith('#'):
                    continue  # Skip comment lines
                parts = line.strip().split()
                if len(parts) == 14:  # Ensure the line has the expected number of fields
                    # Replace '-' with None for missing fields
                    parts = [None if field == '-' else field for field in parts]
                    combined_data.append(parts)
                else:
                    print(f"Skipping invalid line in {log_file}: {line.strip()}")

    # Define expected columns
    columns = ["date", "time", "s-ip", "cs-method", "cs-uri-stem", "cs-uri-query", "s-port",
               "cs-username", "c-ip", "cs(User-Agent)", "sc-status", "sc-substatus", 
               "sc-win32-status", "time-taken"]

    # Create DataFrame
    if combined_data:
        df = pd.DataFrame(combined_data, columns=columns[:len(combined_data[0])])
    else:
        print("No valid log entries found. Creating an empty DataFrame.")
        df = pd.DataFrame(columns=columns)

    # Save combined CSV
    combined_csv_path = STAGE_DIR / "CombinedLogs.csv"
    df.to_csv(combined_csv_path, index=False)
    print(f"Combined logs saved to: {combined_csv_path}")

if __name__ == "__main__":
    process_logs_to_csv()
