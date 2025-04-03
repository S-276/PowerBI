import os
import pandas as pd
import requests
import json
from pathlib import Path
import time  # Add this import for periodic heartbeats

print("🟢 Running updated log_to_csv.py script...")


# Define directories
LOG_DIR = Path("/opt/airflow/data/ISSLogs")
STAGE_DIR = Path("/opt/airflow/data/StagingArea")
CACHE_FILE = STAGE_DIR / "geoip_cache.json"  # Cache file for GeoIP results
IPLOCATION_API_URL = "https://api.iplocation.net/?ip={ip}"

# Load or initialize the GeoIP cache
if CACHE_FILE.exists():
    with open(CACHE_FILE, "r") as f:
        GEOIP_CACHE = json.load(f)
else:
    GEOIP_CACHE = {}

def save_geoip_cache():
    """Save the GeoIP cache to a file."""
    with open(CACHE_FILE, "w") as f:
        json.dump(GEOIP_CACHE, f)

def get_geoip_data(ip_address: str) -> dict:
    """Fetch GeoIP data for a given IP address using iplocation.net."""
    if ip_address in GEOIP_CACHE:
        return GEOIP_CACHE[ip_address]

    try:
        response = requests.get(IPLOCATION_API_URL.format(ip=ip_address))
        if response.status_code == 200:
            data = response.json()
            geoip_data = {
                "country_name": data.get("country_name"),
                "region_name": data.get("region_name"),
                "city": data.get("city"),
            }
            GEOIP_CACHE[ip_address] = geoip_data
            return geoip_data
        else:
            print(f"GeoIP API failed for IP {ip_address}: {response.status_code}")
    except Exception as e:
        print(f"GeoIP API exception for IP {ip_address}: {e}")

    # Return default values if the API fails
    GEOIP_CACHE[ip_address] = {"country_name": None, "region_name": None, "city": None}
    return GEOIP_CACHE[ip_address]

def process_logs_to_csv():
    """Process all log files in LOG_DIR and combine them into a single CSV."""
    try:
        print("Starting log processing...")
        STAGE_DIR.mkdir(parents=True, exist_ok=True)

        # Ensure the directory is writable
        if not os.access(STAGE_DIR, os.W_OK):
            raise PermissionError(f"Write permission denied for directory: {STAGE_DIR}")

        combined_data = []
        total_entries = 0
        unique_ips = set()

        log_files = sorted(LOG_DIR.glob("*.log"))
        if not log_files:
            print(f"⚠️ No log files found in the directory: {LOG_DIR}")
            pd.DataFrame(columns=["c-ip", "date", "time", "cs-uri-stem", "sc-status"]).to_csv(
                STAGE_DIR / "CombinedLogs.csv", index=False
            )
            return

        print(f"Files found: {len(log_files)}")

        for log_file in log_files:
            print(f"Processing: {log_file}")
            valid_count = 0
            skip_count = 0
            expected_fields = []
            delimiter = " "

            try:
                with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        line = line.strip()

                        if not line:
                            continue

                        if line.startswith("#Fields:"):
                            expected_fields = line.replace("#Fields:", "").strip().split()
                            continue

                        if line.startswith("#"):
                            continue

                        if "\t" in line:
                            delimiter = "\t"

                        parts = line.split(delimiter)

                        if expected_fields and len(parts) == len(expected_fields):
                            cleaned = [None if x == '-' else x for x in parts]
                            ip_address = cleaned[expected_fields.index("c-ip")]
                            unique_ips.add(ip_address)
                            combined_data.append(cleaned)
                            valid_count += 1
                        else:
                            skip_count += 1

                print(f"{log_file.name}: ✅ {valid_count} valid, ❌ {skip_count} skipped")
                total_entries += valid_count

            except Exception as e:
                print(f"⚠️ Error processing file {log_file}: {e}")

        print(f"\n📊 TOTAL valid entries across all log files: {total_entries}")
        print(f"📊 Unique IPs found: {len(unique_ips)}")

        # Perform GeoIP lookups for unique IPs
        print("Performing GeoIP lookups for unique IPs...")
        for i, ip in enumerate(unique_ips):
            get_geoip_data(ip)
            if i % 100 == 0:
                print(f"Processed {i}/{len(unique_ips)} IPs...")
                time.sleep(1)

        save_geoip_cache()

        # Enrich combined data with GeoIP information
        print("Enriching log data with GeoIP information...")
        enriched_data = []
        for row in combined_data:
            ip_address = row[expected_fields.index("c-ip")]
            geoip_data = GEOIP_CACHE.get(ip_address, {"city": None, "region_name": None, "country_name": None})
            row.extend([geoip_data["city"], geoip_data["region_name"], geoip_data["country_name"]])
            enriched_data.append(row)

        # Final columns after enrichment
        if "city" not in expected_fields:
            expected_fields += ["city", "region_name", "country_name"]
        df = pd.DataFrame(enriched_data, columns=expected_fields)


        # Save to CSV
        combined_csv_path = STAGE_DIR / "CombinedLogs.csv"
        df.to_csv(combined_csv_path, index=False)
        print(f"✅ Combined CSV created at: {combined_csv_path}")

    except Exception as e:
        print(f"⚠️ Unexpected error during log processing: {e}")

if __name__ == "__main__":
    process_logs_to_csv()
