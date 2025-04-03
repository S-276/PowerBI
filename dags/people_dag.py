import datetime as dt
import csv
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

TMP_DIR = Variable.get("tmp_dir", default_var="/tmp")
DATA_DIR = Variable.get("data_dir", default_var="D:/Documents/UK/University/BI/Repo/airflow-etl/data")

def validate_input_file(input_path):
    """Validate the input file exists and is not empty."""
    if not os.path.exists(input_path):
        raise AirflowException(f"Input file {input_path} does not exist.")
    if os.path.getsize(input_path) == 0:
        raise AirflowException(f"Input file {input_path} is empty.")

def transform_data_from_logs(**kwargs):
    """Transform log data into a structured format."""
    log_dir = DATA_DIR
    output_path = os.path.join(TMP_DIR, "people.txt")

    try:
        with open(output_path, 'w') as outfile:
            outfile.write("First,Last,Email\n")
            for log_file in os.listdir(log_dir):
                if log_file.endswith(".log"):
                    kwargs['ti'].log.info(f"Processing log file: {log_file}")
                    with open(os.path.join(log_dir, log_file), 'r') as infile:
                        for line in infile:
                            # Example log parsing logic (adjust based on your log format)
                            parts = line.strip().split(" ")
                            if len(parts) >= 3:  # Adjust based on log structure
                                first, last, email = parts[0], parts[1], parts[2]
                                outfile.write(f"{first},{last},{email}\n")
    except Exception as e:
        raise AirflowException(f"Failed to transform log data: {str(e)}")

def load_data_to_postgres(**kwargs):
    """Load transformed data into PostgreSQL."""
    input_path = os.path.join(TMP_DIR, "people.txt")
    
    validate_input_file(input_path)  # Validate the transformed file

    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    try:
        with open(input_path, 'r') as infile:
            next(infile)  # Skip header
            for line in infile:
                first, last, email = line.strip().split(",")
                cursor.execute(
                    "INSERT INTO people (first_name, last_name, email) VALUES (%s, %s, %s)",
                    (first, last, email),
                )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise AirflowException(f"Failed to load data into PostgreSQL: {str(e)}")
    finally:
        cursor.close()
        conn.close()

def ensure_people_table(**kwargs):
    """Ensure the `people` table exists in PostgreSQL."""
    CREATE_PEOPLE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS people (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        email VARCHAR(255)
    );
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        kwargs['ti'].log.info("Attempting to connect to PostgreSQL...")
        postgres_hook.run(CREATE_PEOPLE_TABLE_SQL)
        kwargs['ti'].log.info("Successfully ensured the `people` table exists.")
    except Exception as e:
        kwargs['ti'].log.error(f"Failed to ensure people table: {str(e)}")
        raise AirflowException(f"Failed to ensure people table: {str(e)}")

def extract_log_data(**kwargs):
    """Extract information from log files and save to a structured format."""
    log_dir = DATA_DIR
    output_path = os.path.join(DATA_DIR, "log_summary.csv")

    try:
        with open(output_path, 'w') as outfile:
            outfile.write("Timestamp,LogLevel,Message\n")
            for log_file in os.listdir(log_dir):
                if log_file.endswith(".log"):
                    with open(os.path.join(log_dir, log_file), 'r') as infile:
                        for line in infile:
                            # Example log parsing logic (adjust based on your log format)
                            parts = line.strip().split(" ", 2)
                            if len(parts) == 3:
                                timestamp, log_level, message = parts
                                outfile.write(f"{timestamp},{log_level},{message}\n")
    except Exception as e:
        raise AirflowException(f"Failed to extract log data: {str(e)}")

def ensure_tmp_dir(**kwargs):
    """Ensure the temporary directory exists."""
    if not os.path.exists(TMP_DIR):
        os.makedirs(TMP_DIR)
        kwargs['ti'].log.info(f"Created temporary directory: {TMP_DIR}")
    else:
        kwargs['ti'].log.info(f"Temporary directory already exists: {TMP_DIR}")

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=3),
    'start_date': dt.datetime(2024, 1, 1),
}

with DAG(
    dag_id="People_Data_Pipeline",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
) as dag:

    ensure_tmp_dir_task = PythonOperator(
        task_id="ensure_tmp_dir",
        python_callable=ensure_tmp_dir,
    )

    ensure_table_task = PythonOperator(  # Ensure this task is defined
        task_id="ensure_people_table",
        python_callable=ensure_people_table,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data_from_logs,
    )

    copy_task = BashOperator(
        task_id="archive_result",
        bash_command=f"cp {TMP_DIR}/people.txt {DATA_DIR}/people_$(date +'%Y%m%d').txt",
    )

    upload_task = PythonOperator(
        task_id="upload_to_postgres",
        python_callable=load_data_to_postgres,
    )

    extract_logs_task = PythonOperator(
        task_id="extract_log_data",
        python_callable=extract_log_data,
    )

    # Ensure the task dependencies are correct
    ensure_tmp_dir_task >> ensure_table_task >> transform_task >> copy_task >> upload_task >> extract_logs_task