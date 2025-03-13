import pandas as pd

def validate_dataframes(users_df, streams_df, tracks_df):
    """
    Validates Users, Streams, and Tracks DataFrames.

    Returns a dictionary with validation results.
    """
    validation_results = {}

    # 1️⃣ Validate Users DataFrame
    validation_results["users"] = {
        "missing_values": users_df.isnull().sum().to_dict(),
        "duplicate_rows": users_df.duplicated().sum(),
        "data_types": users_df.dtypes.to_dict(),
        "invalid_user_ids": users_df[~users_df["user_id"].astype(str).str.isnumeric()].shape[0]
    }

    # 2️⃣ Validate Streams DataFrame
    validation_results["streams"] = {
        "missing_values": streams_df.isnull().sum().to_dict(),
        "duplicate_rows": streams_df.duplicated().sum(),
        "data_types": streams_df.dtypes.to_dict(),
        "invalid_timestamps": streams_df[~pd.to_datetime(streams_df["timestamp"], errors='coerce').notna()].shape[0],
        "invalid_user_ids": streams_df[~streams_df["user_id"].astype(str).str.isnumeric()].shape[0],
        "invalid_track_ids": streams_df[~streams_df["track_id"].astype(str).str.isnumeric()].shape[0]
    }

    # 3️⃣ Validate Tracks DataFrame
    validation_results["tracks"] = {
        "missing_values": tracks_df.isnull().sum().to_dict(),
        "duplicate_rows": tracks_df.duplicated().sum(),
        "data_types": tracks_df.dtypes.to_dict(),
        "invalid_track_ids": tracks_df[~tracks_df["track_id"].astype(str).str.isnumeric()].shape[0]
    }

    return validation_results
