from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
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

    def save_to_s3(df, key):
        """Saves a DataFrame as CSV to S3."""
        s3_hook = S3Hook(aws_conn_id="aws_default")
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_hook.load_string(csv_buffer.getvalue(), key, bucket_name=S3_BUCKET, replace=True)

    def load_from_s3(key):
        """Loads a CSV file from S3 into a DataFrame."""
        s3_hook = S3Hook(aws_conn_id="aws_default")
        obj = s3_hook.get_key(key, bucket_name=S3_BUCKET)
        return pd.read_csv(io.StringIO(obj.get()["Body"].read().decode()))

    @task
    def fetch_rds_data(**kwargs):
        """Fetches users and tracks data from PostgreSQL RDS and saves them to S3 as CSV."""
        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = postgres_hook.get_sqlalchemy_engine()

        users_df = pd.read_sql("SELECT * FROM users;", engine)
        tracks_df = pd.read_sql("SELECT * FROM tracks;", engine)

        users_key = f"{S3_PREFIX}users.csv"
        tracks_key = f"{S3_PREFIX}tracks.csv"

        save_to_s3(users_df, users_key)
        save_to_s3(tracks_df, tracks_key)
        data ={"users_key": 'stagingData/users.csv', "tracks_key": 'stagingData/tracks.csv'}
        ti = kwargs['ti']
        ti.xcom_push(key='users_key', value=data['users_key'])
        ti.xcom_push(key='tracks_key', value=data['tracks_key'])


    @task
    def fetch_s3_data():
        """Fetches streaming data from S3, merges it, and saves as CSV."""
        stream_key = f"{S3_PREFIX}streaming.csv"
        df = fetch_and_merge_csvs_from_s3(S3_BUCKET, "streaming-data/")
        save_to_s3(df, stream_key)
        return stream_key
    @task
    def validate_data(**kwargs):
        """Validates Users, Streams, and Tracks DataFrames."""
        ti = kwargs['ti']
        users_key = ti.xcom_pull(task_ids='fetch_rds_data', key='users_key')
        tracks_key = ti.xcom_pull(task_ids='fetch_rds_data', key='tracks_key')
        stream_key = ti.xcom_pull(task_ids='fetch_s3_data')

        users_df = load_from_s3("s3://{S3_BUCKET}/{users_key}")
        tracks_df = load_from_s3("s3://{S3_BUCKET}/{tracks_key}")
        streams_df = load_from_s3("s3://{S3_BUCKET}/{stream_key}")
        
    

        return validate_dataframes(users_df, streams_df, tracks_df)
    
    @task
    def clean_data(users_key, tracks_key, stream_key, **kwargs):
        """Cleans metadata for users, tracks, and stream data, then saves to S3 as CSV."""
        ti = kwargs['ti']
        users_key = ti.xcom_pull(task_ids='fetch_rds_data', key='users_key')
        tracks_key = ti.xcom_pull(task_ids='fetch_rds_data', key='tracks_key')
        if not users_key or not tracks_key:
            raise ValueError("Missing XCom data from fetch_rds_data")
        
        print(f"Processing files: {users_key}, {tracks_key}")
        users_df = clean_metadata(load_from_s3(users_key))
        tracks_df = clean_metadata(load_from_s3(tracks_key))
        stream_df = clean_metadata(load_from_s3(stream_key))

        cleaned_users_key = f"{S3_PREFIX}cleaned_users.csv"
        cleaned_tracks_key = f"{S3_PREFIX}cleaned_tracks.csv"
        cleaned_stream_key = f"{S3_PREFIX}cleaned_stream.csv"

        ti.xcom_push(key='cleaned_users_key', value=cleaned_users_key)
        ti.xcom_push(key='cleaned_tracks_key', value=cleaned_tracks_key)
        ti.xcom_push(key='cleaned_stream_key', value=cleaned_stream_key)

        save_to_s3(users_df, cleaned_users_key)
        save_to_s3(tracks_df, cleaned_tracks_key)
        save_to_s3(stream_df, cleaned_stream_key)

        return {
            "cleaned_users_key": cleaned_users_key,
            "cleaned_tracks_key": cleaned_tracks_key,
            "cleaned_stream_key": cleaned_stream_key
        }

    @task
    def compute_kpis(cleaned_tracks_key, cleaned_stream_key, **kwargs):
        """Computes Genre KPIs and Hourly KPIs, then saves them to S3 as CSV."""
        ti = kwargs['ti']
        cleaned_tracks_key = ti.xcom_pull(task_ids='clean_data', key='cleaned_tracks_key')
        cleaned_stream_key = ti.xcom_pull(task_ids='clean_data', key='cleaned_stream_key')
        if not cleaned_tracks_key or not cleaned_stream_key:
            raise ValueError("Missing XCom data from clean_data")

        print(f"Processing files: {cleaned_tracks_key}, {cleaned_stream_key}")  
        tracks_df = load_from_s3(cleaned_tracks_key)
        stream_df = load_from_s3(cleaned_stream_key)

        genre_kpis = compute_genre_kpis(tracks_df, stream_df)
        hourly_kpis = compute_hourly_kpis(stream_df, tracks_df)

        genre_kpis_key = f"{S3_PREFIX}genre_kpis.csv"
        hourly_kpis_key = f"{S3_PREFIX}hourly_kpis.csv"

        save_to_s3(genre_kpis, genre_kpis_key)
        save_to_s3(hourly_kpis, hourly_kpis_key)

        ti.xcom_push(key='genre_kpis_key', value=genre_kpis_key)
        ti.xcom_push(key='hourly_kpis_key', value=hourly_kpis_key)

        return {"genre_kpis_key": genre_kpis_key, "hourly_kpis_key": hourly_kpis_key}

    @task
    def load_kpis_genre_to_redshift(genre_kpis_key):
        """Loads a CSV from S3 to Redshift using psycopg2."""
        try:
            # Establish connection to Redshift
            conn = psycopg2.connect(
                host=HOST,
                port=PORT,
                dbname=DATABASE,
                user=USER,
                password=PASSWORD
            )
            cursor = conn.cursor()

            # Create table if it does not exist
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS public.kpis_genre (
                track_genre VARCHAR(100),
                listen_count INTEGER,
                average_track_duration DECIMAL(10,2), 
                popularity_index DECIMAL(10,2), 
                track_id VARCHAR(50)
            );
            """
            cursor.execute(create_table_sql)
            conn.commit()
            print(f"Table kpi_hourly checked/created successfully.")

            # COPY command to load data
            copy_sql = f"""
            COPY public.kpis_hourly
            FROM 's3://{S3_BUCKET}/{genre_kpis_key}'
            IAM_ROLE '{IAM_ROLE}'
            FORMAT AS CSV
            DELIMITER ','
            IGNOREHEADER 1;
            """
            cursor.execute(copy_sql)
            conn.commit()
            print(f"Data successfully loaded into kpis_hourly from genre_kpi.csv.")

            # Close connection
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Error loading data: {e}")

    @task
    def load_kpis_hourly_to_redshift(hourly_kpis_key):
        """Loads kpis_hourly CSV from S3 to Redshift using psycopg2."""
        try:
            # Establish connection to Redshift
            conn = psycopg2.connect(
                host=HOST,
                port=PORT,
                dbname=DATABASE,
                user=USER,
                password=PASSWORD
            )
            cursor = conn.cursor()

            # Create table if it does not exist
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS public.kpis_hourly (
                hour INTEGER,
                unique_listeners INTEGER,
                top_artist_per_hour VARCHAR(255),
                track_diversity_index DECIMAL(10,6)
            );
            """
            cursor.execute(create_table_sql)
            conn.commit()
            print("Table kpis_hourly checked/created successfully.")

            # COPY command to load data
            copy_sql = f"""
            COPY public.kpis_hourly
            FROM 's3://{S3_BUCKET}/{hourly_kpis_key}'
            IAM_ROLE '{IAM_ROLE}'
            FORMAT AS CSV
            DELIMITER ','
            IGNOREHEADER 1;
            """
            cursor.execute(copy_sql)
            conn.commit()
            print(f"Data successfully loaded into kpis_hourly from hourly_kpi.csv.")

            # Close connection
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Error loading data: {e}")


    # **Task Dependencies**
  
    fetch_rds_data_task = fetch_rds_data()
    fetch_s3_data_task = fetch_s3_data()

    cleaned_data_task = clean_data(
        users_key=fetch_rds_data_task["users_key"], 
        tracks_key=fetch_rds_data_task["tracks_key"], 
        stream_key=fetch_s3_data_task
    )
    validate_data(users_key=fetch_rds_data_task["users_key"], tracks_key=fetch_rds_data_task["tracks_key"], stream_key=fetch_s3_data_task)

    kpis_task = compute_kpis(
        cleaned_tracks_key=cleaned_data_task["cleaned_tracks_key"], 
        cleaned_stream_key=cleaned_data_task["cleaned_stream_key"]
    )

    load_kpis_genre_to_redshift(genre_kpis_key=kpis_task["genre_kpis_key"],)

    load_kpis_hourly_to_redshift(hourly_kpis_key=kpis_task["hourly_kpis_key"],)

music_streaming_kpi_pipeline()
