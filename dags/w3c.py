import datetime as dt
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from functools import lru_cache  # For caching GeoIP results
from airflow.operators.python import PythonOperator

BASE_DIR = Path("/opt/airflow/data")  # Use container path consistently
LOG_DIR = BASE_DIR / "ISSLogs"  # Correctly point to ISSLogs directory
STAGE_DIR = BASE_DIR / "StagingArea"  # Ensure StagingArea is separate from ISSLogs
SCHEMA_DIR = BASE_DIR / "StarSchema"

IP2LOCATION_API_URL = "https://api.iplocation.net/?ip={ip}"

def create_directories():
    """Create required directories with error handling"""
    try:
        SCHEMA_DIR.mkdir(parents=True, exist_ok=True)
        STAGE_DIR.mkdir(parents=True, exist_ok=True)
        (SCHEMA_DIR / "Insights").mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise AirflowException(f"Directory creation failed: {str(e)}")

@lru_cache(maxsize=1000)
def get_geoip_data(ip_address: str) -> dict:
    """Fetch GeoIP data for a given IP address using ip2location."""
    try:
        # Use ip2location API
        response = requests.get(IP2LOCATION_API_URL.format(ip=ip_address))
        if response.status_code == 200:
            data = response.json()
            return {
                "country_name": data.get("country_name"),
                "region_name": data.get("region_name"),
                "city": data.get("city_name"),
            }
        else:
            print(f"GeoIP API failed for IP {ip_address}: {response.status_code}")
    except Exception as e:
        print(f"GeoIP API exception for IP {ip_address}: {e}")

    # Return default values if the API fails
    return {"country_name": None, "region_name": None, "city": None}

def generate_insights(df: pd.DataFrame):
    """Generate insights based on the provided queries and save them."""
    insights_dir = SCHEMA_DIR / "Insights"

    frequent_visitors = df["c-ip"].value_counts().reset_index()
    frequent_visitors.columns = ["IP Address", "Request Count"]
    frequent_visitors.to_csv(insights_dir / "FrequentVisitors.csv", index=False)

    geo_trends = df.groupby(["country", "region", "city"]).size().reset_index(name="Request Count")
    geo_trends.to_csv(insights_dir / "GeoTrends.csv", index=False)

    slow_pages = df[["cs-uri-stem", "time-taken"]].groupby("cs-uri-stem").mean().reset_index()
    slow_pages.columns = ["Page", "Average Load Time"]
    slow_pages.sort_values(by="Average Load Time", ascending=False).to_csv(insights_dir / "SlowPages.csv", index=False)

    error_monitoring = df[df["sc-status"].str.startswith(("4", "5"))]
    error_counts = error_monitoring["sc-status"].value_counts().reset_index()
    error_counts.columns = ["Error Code", "Count"]
    error_counts.to_csv(insights_dir / "ErrorMonitoring.csv", index=False)

    errors_by_device = error_monitoring["cs(User-Agent)"].value_counts().reset_index()
    errors_by_device.columns = ["User Agent", "Error Count"]
    errors_by_device.to_csv(insights_dir / "ErrorsByDevice.csv", index=False)

    bot_traffic = df[df["cs-uri-stem"].str.contains("robots.txt", na=False)]
    bot_traffic_ips = bot_traffic["c-ip"].value_counts().reset_index()
    bot_traffic_ips.columns = ["IP Address", "Access Count"]
    bot_traffic_ips.to_csv(insights_dir / "BotTraffic.csv", index=False)

    top_referrers = df["cs(User-Agent)"].value_counts().reset_index()
    top_referrers.columns = ["User Agent", "Request Count"]
    top_referrers.to_csv(insights_dir / "TopReferrers.csv", index=False)

    df["DayOfWeek"] = df["FullDate"].dt.day_name()
    traffic_patterns = df["DayOfWeek"].value_counts().reset_index()
    traffic_patterns.columns = ["Day of Week", "Request Count"]
    traffic_patterns.to_csv(insights_dir / "TrafficPatterns.csv", index=False)

def process_logs(**kwargs):
    """Main processing function to parse the combined CSV of all logs and generate dimension tables."""
    create_directories()

    try:
        # Path to the combined CSV
        combined_csv_path = STAGE_DIR / "CombinedLogs.csv"

        # Check if the combined CSV exists
        if not combined_csv_path.exists():
            raise AirflowException(f"Combined CSV not found at {combined_csv_path}")

        # Read the combined CSV
        print(f"Reading combined CSV from: {combined_csv_path}")
        df = pd.read_csv(combined_csv_path, low_memory=False)
        print(f"Loaded {len(df)} rows from the combined CSV.")

        # Check if the DataFrame is empty
        if df.empty:
            print("No valid data found in the combined CSV. Skipping further processing.")
            return

        # Add derived columns for dimensions
        print("Adding derived columns...")
        df["FullDate"] = pd.to_datetime(df["date"] + " " + df["time"], errors='coerce')
        df["Day"] = df["FullDate"].dt.day
        df["Month"] = df["FullDate"].dt.month
        df["Year"] = df["FullDate"].dt.year
        df["FileType"] = df["cs-uri-stem"].apply(lambda x: x.split('.')[-1] if '.' in x else None)

        # Ensure sc-status is a string before applying .startswith()
        df["sc-status"] = df["sc-status"].astype(str)
        df["ErrorType"] = df["sc-status"].apply(lambda x: "ClientError" if x.startswith("4") else "ServerError" if x.startswith("5") else None)

        df["UserAgent"] = df["cs(User-Agent)"].astype(str).str.slice(0, 255)

        # Create Dimensions
        print("Creating dimensions...")
        dim_date = df[["FullDate", "Day", "Month", "Year"]].drop_duplicates().reset_index(drop=True)
        dim_date["DateID"] = dim_date.index + 1

        dim_client = df[["c-ip", "city", "country"]].drop_duplicates().reset_index(drop=True)
        dim_client["ClientID"] = dim_client.index + 1

        dim_request = df[["cs-uri-stem", "FileType"]].drop_duplicates().reset_index(drop=True)
        dim_request["RequestID"] = dim_request.index + 1

        dim_error = df[["sc-status", "ErrorType"]].drop_duplicates().reset_index(drop=True)
        dim_error["ErrorID"] = dim_error.index + 1

        dim_referrer = df[["cs-uri-query"]].drop_duplicates().reset_index(drop=True)
        dim_referrer["ReferrerID"] = dim_referrer.index + 1

        dim_user_agent = df[["UserAgent"]].drop_duplicates().reset_index(drop=True)
        dim_user_agent["UserAgentID"] = dim_user_agent.index + 1

        dim_geolocation = df[["city", "region_name", "country_name"]].drop_duplicates().reset_index(drop=True)
        dim_geolocation["GeolocationID"] = dim_geolocation.index + 1

        # Merge Dimensions into Fact Table
        print("Creating fact table...")
        df = df.merge(dim_date, on="FullDate", how="left")
        df = df.merge(dim_client, on=["c-ip", "city", "country"], how="left")
        df = df.merge(dim_request, on=["cs-uri-stem", "FileType"], how="left")
        df = df.merge(dim_error, on=["sc-status", "ErrorType"], how="left")
        df = df.merge(dim_referrer, on="cs-uri-query", how="left")
        df = df.merge(dim_user_agent, on="UserAgent", how="left")
        df = df.merge(dim_geolocation, on=["city", "region_name", "country_name"], how="left")

        fact_table = df[[
            "FullDate", "time-taken", "ClientID", "DateID", "RequestID",
            "ErrorID", "ReferrerID", "UserAgentID", "GeolocationID"
        ]].drop_duplicates().reset_index(drop=True)

        # Save tables
        print("Saving tables...")
        fact_table.to_csv(SCHEMA_DIR / "FactTable.csv", index=False)
        dim_date.to_csv(SCHEMA_DIR / "DimDate.csv", index=False)
        dim_client.to_csv(SCHEMA_DIR / "DimClient.csv", index=False)
        dim_request.to_csv(SCHEMA_DIR / "DimRequest.csv", index=False)
        dim_error.to_csv(SCHEMA_DIR / "DimError.csv", index=False)
        dim_referrer.to_csv(SCHEMA_DIR / "DimReferrer.csv", index=False)
        dim_user_agent.to_csv(SCHEMA_DIR / "DimUserAgent.csv", index=False)
        dim_geolocation.to_csv(SCHEMA_DIR / "DimGeolocation.csv", index=False)
        print("Star Schema tables saved.")

    except Exception as e:
        raise AirflowException(f"Log processing failed: {str(e)}")

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=5),
    'start_date': dt.datetime(2024, 1, 1),
}

with DAG(
    dag_id="Web_Log_Processing",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task to process logs into a single CSV
    process_logs_task = BashOperator(
        task_id="process_logs_to_csv",
        bash_command="cd /opt/airflow/dags && python3 log_to_csv.py",  # Corrected path
        execution_timeout=timedelta(hours=4),  # Increased timeout to 4 hours
    )

    # Task to generate star schema tables
    generate_star_schema = PythonOperator(
        task_id="generate_star_schema",
        python_callable=process_logs,
    )

    process_logs_task >> generate_star_schema