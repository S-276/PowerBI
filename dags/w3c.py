import datetime as dt
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from functools import lru_cache  # For caching GeoIP results

BASE_DIR = Path(Variable.get("data_dir", default_var="d:/Documents/UK/University/BI/Repo/airflow-etl/data"))
LOG_DIR = BASE_DIR  # Updated to point directly to the log files directory
STAGE_DIR = BASE_DIR / "StagingArea"
SCHEMA_DIR = BASE_DIR / "StarSchema"
GEOIP_API_URL = "https://freegeoip.app/json/"  # Replace with a faster API if needed

def create_directories():
    """Create required directories with error handling"""
    try:
        SCHEMA_DIR.mkdir(parents=True, exist_ok=True)
        STAGE_DIR.mkdir(parents=True, exist_ok=True)
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        (SCHEMA_DIR / "Insights").mkdir(parents=True, exist_ok=True)  # Include Insights directory
    except Exception as e:
        raise AirflowException(f"Directory creation failed: {str(e)}")

@lru_cache(maxsize=1000)
def get_geoip_data(ip_address: str) -> dict:
    """Fetch GeoIP data for a given IP address with caching"""
    try:
        response = requests.get(f"{GEOIP_API_URL}{ip_address}")
        if response.status_code == 200:
            return response.json()
        else:
            return {"country_name": None, "region_name": None, "city": None}
    except Exception:
        return {"country_name": None, "region_name": None, "city": None}

def generate_insights(df: pd.DataFrame):
    """Generate insights based on the provided queries and save them."""
    insights_dir = SCHEMA_DIR / "Insights"

    # 1. Frequent Visitors
    frequent_visitors = df["c-ip"].value_counts().reset_index()
    frequent_visitors.columns = ["IP Address", "Request Count"]
    frequent_visitors.to_csv(insights_dir / "FrequentVisitors.csv", index=False)

    # 2. Geo Trends
    geo_trends = df.groupby(["country", "region", "city"]).size().reset_index(name="Request Count")
    geo_trends.to_csv(insights_dir / "GeoTrends.csv", index=False)

    # 3. Slow Pages
    slow_pages = df[["cs-uri-stem", "time-taken"]].groupby("cs-uri-stem").mean().reset_index()
    slow_pages.columns = ["Page", "Average Load Time"]
    slow_pages = slow_pages.sort_values(by="Average Load Time", ascending=False)
    slow_pages.to_csv(insights_dir / "SlowPages.csv", index=False)

    # 4. Error Monitoring and Errors by Device
    error_monitoring = df[df["sc-status"].str.startswith(("4", "5"))]
    error_counts = error_monitoring["sc-status"].value_counts().reset_index()
    error_counts.columns = ["Error Code", "Count"]
    error_counts.to_csv(insights_dir / "ErrorMonitoring.csv", index=False)

    errors_by_device = error_monitoring["cs(User-Agent)"].value_counts().reset_index()
    errors_by_device.columns = ["User Agent", "Error Count"]
    errors_by_device.to_csv(insights_dir / "ErrorsByDevice.csv", index=False)

    # 5. Bot Traffic
    bot_traffic = df[df["cs-uri-stem"].str.contains("robots.txt", na=False)]
    bot_traffic_ips = bot_traffic["c-ip"].value_counts().reset_index()
    bot_traffic_ips.columns = ["IP Address", "Access Count"]
    bot_traffic_ips.to_csv(insights_dir / "BotTraffic.csv", index=False)

    # 6. Top Referrers
    top_referrers = df["cs(User-Agent)"].value_counts().reset_index()
    top_referrers.columns = ["User Agent", "Request Count"]
    top_referrers.to_csv(insights_dir / "TopReferrers.csv", index=False)

    # 7. Traffic Patterns
    df["DayOfWeek"] = df["FullDate"].dt.day_name()
    traffic_patterns = df["DayOfWeek"].value_counts().reset_index()
    traffic_patterns.columns = ["Day of Week", "Request Count"]
    traffic_patterns.to_csv(insights_dir / "TrafficPatterns.csv", index=False)

def process_logs(**kwargs):
    """Main processing function to parse the combined CSV of all logs"""
    create_directories()
    
    try:
        # Path to the combined CSV
        combined_csv_path = STAGE_DIR / "CombinedLogs.csv"
        
        # Check if the combined CSV exists
        if not combined_csv_path.exists():
            raise AirflowException(f"Combined CSV not found at {combined_csv_path}")
        
        # Read the combined CSV
        print(f"Reading combined CSV from: {combined_csv_path}")
        df = pd.read_csv(combined_csv_path)
        print(f"Loaded {len(df)} rows from the combined CSV.")
        
        # Check if the DataFrame is empty
        if df.empty:
            print("No valid data found in the combined CSV. Skipping further processing.")
            return
        
        # Add GeoIP data
        print("Adding GeoIP data...")
        geoip_data = df["c-ip"].apply(get_geoip_data)
        df["country"] = geoip_data.apply(lambda x: x.get("country_name"))
        df["region"] = geoip_data.apply(lambda x: x.get("region_name"))
        df["city"] = geoip_data.apply(lambda x: x.get("city"))
        
        # Add derived columns for dimensions
        print("Adding derived columns...")
        df["FullDate"] = pd.to_datetime(df["date"] + " " + df["time"], errors='coerce')
        df["FileType"] = df["cs-uri-stem"].apply(lambda x: x.split('.')[-1] if '.' in x else None)
        df["ErrorType"] = df["sc-status"].apply(lambda x: "ClientError" if x.startswith("4") else "ServerError" if x.startswith("5") else None)
        
        # Save updated CSV
        updated_csv_path = STAGE_DIR / "UpdatedCombinedLogs.csv"
        df.to_csv(updated_csv_path, index=False)
        print(f"Updated combined logs saved to: {updated_csv_path}")
        
        # Create Star Schema tables
        print("Creating Star Schema tables...")
        fact_table = df[["FullDate", "c-ip", "cs-method", "cs-uri-stem", "cs-uri-query", "sc-status", 
                         "time-taken", "country", "region", "city"]]
        dim_date = df[["FullDate"]].drop_duplicates().reset_index(drop=True)
        dim_client = df[["c-ip", "city", "country"]].drop_duplicates().reset_index(drop=True)
        dim_request = df[["cs-uri-stem", "FileType"]].drop_duplicates().reset_index(drop=True)
        
        # Save tables
        fact_table.to_csv(SCHEMA_DIR / "FactTable.csv", index=False)
        dim_date.to_csv(SCHEMA_DIR / "DimDate.csv", index=False)
        dim_client.to_csv(SCHEMA_DIR / "DimClient.csv", index=False)
        dim_request.to_csv(SCHEMA_DIR / "DimRequest.csv", index=False)
        print("Star Schema tables saved.")
        
        # Generate insights
        print("Generating insights...")
        generate_insights(df)
        print("Insights generation completed.")
        
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
        bash_command="python /opt/airflow/dags/log_to_csv.py",  # Updated path
    )

    # Task to generate analytics report
    generate_report_task = BashOperator(
        task_id="generate_analytics_report",
        bash_command=f"python {Path(__file__).parent}/analytics.py",
    )

    process_logs_task >> generate_report_task