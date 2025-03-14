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


'''S3_BUCKET = Variable.get("s3_bucket")
S3_PREFIX = Variable.get("bucket_prefix")
IAM_ROLE = Variable.get("iam_arn")

HOST = Variable.get("redshift_endpoint")
PORT = Variable.get("port")
DATABASE = Variable.get("database")
USER = Variable.get("user")
PASSWORD = Variable.get("password")
'''



@dag(
    schedule_interval="@daily",
    start_date=datetime(2025, 3, 12),
    catchup=False,
    tags=["data_pipeline"]
)
def music_streaming_kpi_pipeline():

    def save_to_s3(df, key):
        """
        Saves a DataFrame as CSV to S3.

        Args:
            df (pd.DataFrame): The DataFrame to be saved.
            key (str): The S3 key of the object to be saved.
        """
        s3_hook = S3Hook(aws_conn_id="aws_default")
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_hook.load_string(csv_buffer.getvalue(), key, bucket_name=S3_BUCKET, replace=True)

    def load_from_s3(key):
        """
        Loads a CSV file from S3 into a DataFrame.

        Args:
            key (str): The S3 key of the object to be loaded.

        Returns:
            pd.DataFrame: The DataFrame loaded from the CSV file.
        """
        # Get the S3 hook
        s3_hook = S3Hook(aws_conn_id="aws_default")
        # Load the CSV file as a string
        obj = s3_hook.get_key(key, bucket_name=S3_BUCKET)
        # Convert the string to a DataFrame
        return pd.read_csv(io.StringIO(obj.get()["Body"].read().decode()))

    @task
    def fetch_rds_data(**kwargs):
        """
        Fetches users and tracks data from PostgreSQL RDS and saves them to S3 as CSV.

        This task fetches the users and tracks tables from a PostgreSQL RDS database using the
        PostgresHook and saves them to S3 as CSV files.

        Args:
            **kwargs: The Airflow context object.

        Returns:
            None
        """

        # Get the PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")

        # Get the SQLAlchemy engine
        engine = postgres_hook.get_sqlalchemy_engine()

        # Fetch the users table
        users_df = pd.read_sql("SELECT * FROM users;", engine)

        # Fetch the tracks table
        tracks_df = pd.read_sql("SELECT * FROM tracks;", engine)

        # Define the S3 keys for the CSV files
        users_key = f"{S3_PREFIX}users.csv"
        tracks_key = f"{S3_PREFIX}tracks.csv"

        # Save the DataFrames to S3 as CSV files
        save_to_s3(users_df, users_key)
        save_to_s3(tracks_df, tracks_key)

        # Push the S3 keys to XCom
        ti = kwargs['ti']
        ti.xcom_push(key='users_key', value=users_key)
        ti.xcom_push(key='tracks_key', value=tracks_key)

    @task
    def fetch_s3_data(**kwargs):
        """
        Fetches streaming data from S3, merges it, and saves as CSV.

        This task fetches the streaming data from S3, merges the DataFrames, and
        saves the merged DataFrame to S3 as a CSV file. The S3 key of the CSV
        file is then pushed to XCom.

        Args:
            **kwargs: The Airflow context object.

        Returns:
            None
        """
        stream_key = f"{S3_PREFIX}streaming.csv"
        df = fetch_and_merge_csvs_from_s3(S3_BUCKET, "streaming-data/")
        save_to_s3(df, stream_key)

        ti = kwargs['ti']
        ti.xcom_push(key='stream_key', value=stream_key)

    @task
    def validate_data(**kwargs):
        """
        Validates Users, Streams, and Tracks DataFrames.

        This task loads the Users, Streams, and Tracks DataFrames from S3,
        validates them using the validate_dataframes function, and returns
        the validation results.

        Args:
            **kwargs: The Airflow context object.

        Returns:
            dict: A dictionary with the validation results.
        """
        ti = kwargs['ti']
        users_key = ti.xcom_pull(task_ids='fetch_rds_data', key='users_key')
        tracks_key = ti.xcom_pull(task_ids='fetch_rds_data', key='tracks_key')
        stream_key = ti.xcom_pull(task_ids='fetch_s3_data', key='stream_key')

        users_df = load_from_s3(users_key)
        tracks_df = load_from_s3(tracks_key)
        streams_df = load_from_s3(stream_key)

        return validate_dataframes(users_df, streams_df, tracks_df)

    @task
    def clean_data(**kwargs):
        """
        Cleans metadata and saves cleaned files to S3.

        This task loads the Users, Tracks, and Streams DataFrames from S3,
        cleans the metadata using the clean_metadata function, saves the
        cleaned DataFrames back to S3 as new CSV files, and pushes their S3
        keys to XCom.

        Args:
            **kwargs: The Airflow context object.

        Returns:
            None
        """
        ti = kwargs['ti']
        # Retrieve S3 keys for the data files
        users_key = ti.xcom_pull(task_ids='fetch_rds_data', key='users_key')
        tracks_key = ti.xcom_pull(task_ids='fetch_rds_data', key='tracks_key')
        stream_key = ti.xcom_pull(task_ids='fetch_s3_data', key='stream_key')

        # Load data from S3 and clean metadata
        users_df = clean_metadata(load_from_s3(users_key))
        tracks_df = clean_metadata(load_from_s3(tracks_key))
        stream_df = clean_metadata(load_from_s3(stream_key))

        # Define S3 keys for the cleaned files
        cleaned_users_key = f"{S3_PREFIX}cleaned_users.csv"
        cleaned_tracks_key = f"{S3_PREFIX}cleaned_tracks.csv"
        cleaned_stream_key = f"{S3_PREFIX}cleaned_stream.csv"

        # Save cleaned DataFrames to S3
        save_to_s3(users_df, cleaned_users_key)
        save_to_s3(tracks_df, cleaned_tracks_key)
        save_to_s3(stream_df, cleaned_stream_key)

        # Push cleaned file keys to XCom
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
        # Retrieve S3 keys for the cleaned data files
        cleaned_tracks_key = ti.xcom_pull(task_ids='clean_data', key='cleaned_tracks_key')
        cleaned_stream_key = ti.xcom_pull(task_ids='clean_data', key='cleaned_stream_key')

        # Load the cleaned data from S3
        tracks_df = load_from_s3(cleaned_tracks_key)
        stream_df = load_from_s3(cleaned_stream_key)

        # Compute Genre KPIs and Hourly KPIs using the compute_genre_kpis and compute_hourly_kpis functions
        genre_kpis = compute_genre_kpis(tracks_df, stream_df)
        hourly_kpis = compute_hourly_kpis(stream_df, tracks_df)

        # Save the computed DataFrames to S3
        genre_kpis_key = f"{S3_PREFIX}genre_kpis.csv"
        hourly_kpis_key = f"{S3_PREFIX}hourly_kpis.csv"
        save_to_s3(genre_kpis, genre_kpis_key)
        save_to_s3(hourly_kpis, hourly_kpis_key)

        # Push the S3 keys of the computed DataFrames to XCom
        ti.xcom_push(key='genre_kpis_key', value=genre_kpis_key)
        ti.xcom_push(key='hourly_kpis_key', value=hourly_kpis_key)

    @task
    def load_to_redshift(**kwargs):
        """
        Loads the computed KPIs from S3 to Redshift.

        This task takes the S3 keys of the computed Genre KPIs and Hourly KPIs
        DataFrames, and loads them into the respective Redshift tables using
        the S3ToRedshiftOperator.
        """
        ti = kwargs['ti']
        genre_kpis_key = ti.xcom_pull(task_ids='compute_kpis', key='genre_kpis_key')
        hourly_kpis_key = ti.xcom_pull(task_ids='compute_kpis', key='hourly_kpis_key')

        # Define the S3ToRedshiftOperators for loading the computed KPIs
        # The genre_kpis_to_redshift operator loads the Genre KPIs DataFrame
        # from the S3 key into the public.genre_kpis table in Redshift.
        # The hourly_kpis_to_redshift operator loads the Hourly KPIs DataFrame
        # from the S3 key into the public.hourly_kpis table in Redshift.
        genre_kpis_to_redshift = S3ToRedshiftOperator(
            task_id='genre_kpis_to_redshift',
            schema='public',
            table='genre_kpis',
            s3_bucket=S3_BUCKET,
            s3_key=genre_kpis_key,
            copy_options=['csv'],
            aws_conn_id='aws_default',
            redshift_conn_id='redshift_default',
            iam_role=IAM_ROLE
        )

        hourly_kpis_to_redshift = S3ToRedshiftOperator(
            task_id='hourly_kpis_to_redshift',
            schema='public',
            table='hourly_kpis',
            s3_bucket=S3_BUCKET,
            s3_key=hourly_kpis_key,
            copy_options=['csv'],
            aws_conn_id='aws_default',
            redshift_conn_id='redshift_default',
            iam_role=IAM_ROLE
        )

        # Execute the S3ToRedshiftOperators
        genre_kpis_to_redshift.execute(context=kwargs)
        hourly_kpis_to_redshift.execute(context=kwargs)

    # Define dependencies
    fetch_rds_data_task = fetch_rds_data()
    fetch_s3_data_task = fetch_s3_data()

    clean_data_task = clean_data()
    validate_data_task = validate_data()

    compute_kpis_task = compute_kpis()
    load_to_redshift_task = load_to_redshift()

    fetch_rds_data_task >> clean_data_task >> validate_data_task >> compute_kpis_task >> load_to_redshift_task
    fetch_s3_data_task >> clean_data_task >> validate_data_task >> compute_kpis_task >> load_to_redshift_task

music_streaming_kpi_pipeline()
