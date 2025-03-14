from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime
import pandas as pd
import io
import psycopg2
from airflow.models import Variable


from helpers.extract_data_rds import fetch_users_and_tracks
from helpers.extract_data_s3 import fetch_and_merge_csvs_from_s3
from helpers.transform_data import clean_metadata, compute_genre_kpis, compute_hourly_kpis
from helpers.validate_dataframes import validate_dataframes


S3_BUCKET = Variable.get("s3_bucket")
S3_PREFIX = Variable.get("bucket_prefix")
IAM_ROLE = Variable.get("iam_arn")

HOST = Variable.get("redshift_endpoint")
PORT = Variable.get("port")
DATABASE = Variable.get("database")
USER = Variable.get("user")
PASSWORD = Variable.get("password")




@dag(
    schedule_interval="@daily",
    start_date=datetime(2025, 3, 12),
    catchup=False,
    tags=["data_pipeline"]
)
def music_streaming_kpi_pipeline():

    """
    DAG for extracting data from PostgreSQL RDS and Amazon S3, processing and computing KPIs, and loading to Amazon Redshift.

    This DAG fetches user and track metadata from PostgreSQL RDS, streaming data from Amazon S3, and validates the data. It then cleans the data, computes genre and hourly KPIs, and loads the KPIs into Amazon Redshift.
    """
    def save_to_s3(df, key):
        """
        Saves a DataFrame as CSV to S3.

        :param df: The DataFrame to save
        :param key: The key to save the file as in S3
        """
        # Create an S3 hook to communicate with S3
        s3_hook = S3Hook(aws_conn_id="aws_default")
        
        # Create a string buffer to hold the CSV data
        csv_buffer = io.StringIO()
        
        # Write the DataFrame to the string buffer as a CSV
        df.to_csv(csv_buffer, index=False)
        
        # Load the string buffer into S3
        s3_hook.load_string(csv_buffer.getvalue(), key, bucket_name=S3_BUCKET, replace=True)

    def load_from_s3(key):
        """
        Loads a CSV file from S3 into a DataFrame.

        :param key: The key of the CSV file in S3 to load
        :return: DataFrame containing the CSV data
        """
        # Create an S3 hook to interact with S3
        s3_hook = S3Hook(aws_conn_id="aws_default")
        
        # Retrieve the S3 object using the specified key
        obj = s3_hook.get_key(key, bucket_name=S3_BUCKET)
        
        # Read the object's contents into a DataFrame
        return pd.read_csv(io.StringIO(obj.get()["Body"].read().decode()))

    @task
    def fetch_rds_data(**kwargs):
        """
        Fetches users and tracks data from PostgreSQL RDS and saves them to S3 as CSV.

        This task fetches the users and tracks tables from PostgreSQL RDS using the
        PostgresHook and saves them to S3 as CSV files.
        """
        # Create a PostgresHook to connect to the RDS instance
        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")

        # Get the SQLAlchemy engine from the PostgresHook
        engine = postgres_hook.get_sqlalchemy_engine()

        # Fetch the users table from the RDS instance
        users_df = pd.read_sql("SELECT * FROM users;", engine)

        # Fetch the tracks table from the RDS instance
        tracks_df = pd.read_sql("SELECT * FROM tracks;", engine)

        # Save the users DataFrame to S3 as CSV
        users_key = f"{S3_PREFIX}users.csv"
        save_to_s3(users_df, users_key)

        # Save the tracks DataFrame to S3 as CSV
        tracks_key = f"{S3_PREFIX}tracks.csv"
        save_to_s3(tracks_df, tracks_key)

        # Push the S3 keys to the task instance for later use
        ti = kwargs['ti']
        ti.xcom_push(key='users_key', value=users_key)
        ti.xcom_push(key='tracks_key', value=tracks_key)

    @task
    def fetch_s3_data(**kwargs):
        """
        Fetches streaming data from S3, merges it, and saves as CSV.

        This task fetches the streaming data from S3, merges the individual CSV files into a single DataFrame, and saves the merged DataFrame as a new CSV file back to S3.
        """
        stream_key = f"{S3_PREFIX}streaming.csv"

        # Fetch the streaming data from S3 and merge it
        df = fetch_and_merge_csvs_from_s3(S3_BUCKET, "streaming-data/")

        # Save the merged DataFrame to S3 as a CSV file
        save_to_s3(df, stream_key)

        # Push the S3 key to the task instance for later use
        ti = kwargs['ti']
        ti.xcom_push(key='stream_key', value=stream_key)

    @task
    def validate_data(**kwargs):
        """
        Validates Users, Streams, and Tracks DataFrames.

        This task takes the S3 keys of the Users, Streams, and Tracks DataFrames,
        loads them from S3, and validates them using the validate_dataframes
        function.

        Returns a dictionary with validation results.
        """
        ti = kwargs['ti']
        users_key = ti.xcom_pull(task_ids='fetch_rds_data', key='users_key')
        tracks_key = ti.xcom_pull(task_ids='fetch_rds_data', key='tracks_key')
        stream_key = ti.xcom_pull(task_ids='fetch_s3_data', key='stream_key')

        # Load the DataFrames from S3
        users_df = load_from_s3(users_key)
        tracks_df = load_from_s3(tracks_key)
        streams_df = load_from_s3(stream_key)

        # Validate the DataFrames
        return validate_dataframes(users_df, streams_df, tracks_df)

    @task
    def clean_data(**kwargs):
        """
        Cleans metadata (user or song metadata) and saves cleaned files to S3.

        This task takes the S3 keys of the Users, Tracks, and Streams DataFrames,
        loads them from S3, cleans them using the clean_metadata function,
        and saves the cleaned DataFrames back to S3 as new CSV files.

        Returns a dictionary with the S3 keys of the cleaned DataFrames.
        """
        ti = kwargs['ti']
        users_key = ti.xcom_pull(task_ids='fetch_rds_data', key='users_key')
        tracks_key = ti.xcom_pull(task_ids='fetch_rds_data', key='tracks_key')
        stream_key = ti.xcom_pull(task_ids='fetch_s3_data', key='stream_key')

        # Load the DataFrames from S3
        users_df = load_from_s3(users_key)
        tracks_df = load_from_s3(tracks_key)
        stream_df = load_from_s3(stream_key)

        # Clean the DataFrames
        users_df = clean_metadata(users_df)
        tracks_df = clean_metadata(tracks_df)
        stream_df = clean_metadata(stream_df)

        # Define the S3 keys for the cleaned DataFrames
        cleaned_users_key = f"{S3_PREFIX}cleaned_users.csv"
        cleaned_tracks_key = f"{S3_PREFIX}cleaned_tracks.csv"
        cleaned_stream_key = f"{S3_PREFIX}cleaned_stream.csv"

        # Save the cleaned DataFrames to S3
        save_to_s3(users_df, cleaned_users_key)
        save_to_s3(tracks_df, cleaned_tracks_key)
        save_to_s3(stream_df, cleaned_stream_key)

        # Push the S3 keys to the task instance for later use
        ti.xcom_push(key='cleaned_users_key', value=cleaned_users_key)
        ti.xcom_push(key='cleaned_tracks_key', value=cleaned_tracks_key)
        ti.xcom_push(key='cleaned_stream_key', value=cleaned_stream_key)

    @task
    def compute_kpis(**kwargs):
        """
        Computes Genre KPIs and Hourly KPIs.

        This task takes the S3 keys of the cleaned Tracks and Streams DataFrames,
        loads them from S3, computes the Genre KPIs and Hourly KPIs using the
        compute_genre_kpis and compute_hourly_kpis functions, and saves the
        computed DataFrames back to S3 as new CSV files.

        Returns a dictionary with the S3 keys of the computed DataFrames.
        """
        ti = kwargs['ti']
        cleaned_tracks_key = ti.xcom_pull(task_ids='clean_data', key='cleaned_tracks_key')
        cleaned_stream_key = ti.xcom_pull(task_ids='clean_data', key='cleaned_stream_key')

        # Load the DataFrames from S3
        tracks_df = load_from_s3(cleaned_tracks_key)
        stream_df = load_from_s3(cleaned_stream_key)

        # Compute the Genre KPIs
        genre_kpis = compute_genre_kpis(tracks_df, stream_df)

        # Compute the Hourly KPIs
        hourly_kpis = compute_hourly_kpis(stream_df, tracks_df)

        # Define the S3 keys for the computed DataFrames
        genre_kpis_key = f"{S3_PREFIX}genre_kpis.csv"
        hourly_kpis_key = f"{S3_PREFIX}hourly_kpis.csv"

        # Save the computed DataFrames to S3
        save_to_s3(genre_kpis, genre_kpis_key)
        save_to_s3(hourly_kpis, hourly_kpis_key)

        # Push the S3 keys to the task instance for later use
        ti.xcom_push(key='genre_kpis_key', value=genre_kpis_key)
        ti.xcom_push(key='hourly_kpis_key', value=hourly_kpis_key)

    # Define dependencies
    fetch_rds_data_task = fetch_rds_data()
    fetch_s3_data_task = fetch_s3_data()

    clean_data_task = clean_data()
    validate_data_task = validate_data()

    compute_kpis_task = compute_kpis()

music_streaming_kpi_pipeline()
