import boto3
import pandas as pd
from io import StringIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def fetch_and_merge_csvs_from_s3(s3_bucket_name: str, s3_folder_prefix: str):
    """Fetches streaming data from S3."""
    s3_hook = S3Hook(aws_conn_id="aws_default")
    bucket_name = "raw-data-gyenyame"
    folder_prefix = "streams/"

    file_keys = s3_hook.list_keys(bucket_name, prefix=folder_prefix)
    dataframes = []

    for key in file_keys:
        if key.endswith(".csv"):
            obj = s3_hook.get_key(key, bucket_name)
            df = pd.read_csv(StringIO(obj.get()['Body'].read().decode('utf-8')))
            dataframes.append(df)

        if dataframes:
            return pd.concat(dataframes, ignore_index=True)
        return pd.DataFrame()

