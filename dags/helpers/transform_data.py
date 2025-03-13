import pandas as pd
import numpy as np
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def clean_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """ Cleans and standardizes metadata (user or song metadata). """
    df = df.dropna()  # Drop missing values
    df.columns = df.columns.str.lower().str.replace(" ", "_")  # Standardize column names
    return df

def compute_genre_kpis(song_df: pd.DataFrame, stream_df: pd.DataFrame) -> pd.DataFrame:
    """ Computes genre-level KPIs. """
      # Convert popularity to numeric (handle errors)
    song_df["popularity"] = pd.to_numeric(song_df["popularity"], errors="coerce")


    merged_df = stream_df.merge(song_df, on="track_id", how="left")
    merged_df["play_count"] = merged_df.groupby("track_id")["track_id"].transform("count")
    merged_df["engagement_score"] = merged_df["duration_ms"] * merged_df["popularity"]/100

    genre_kpis = merged_df.groupby("track_genre").agg(
        listen_count=("play_count", "sum"),
        avg_track_duration=("duration_ms", "mean"),
        popularity_index=("engagement_score", "sum")
    ).reset_index()
    
    # Most Popular Track per Genre
    popular_tracks = merged_df.loc[merged_df.groupby("track_genre")["engagement_score"].idxmax(), ["track_genre", "track_id"]]
    genre_kpis = genre_kpis.merge(popular_tracks, on="track_genre", how="left")
    
    return genre_kpis

def compute_hourly_kpis(stream_df: pd.DataFrame, song_df: pd.DataFrame) -> pd.DataFrame:
    """ Computes hourly KPIs. """
    # Convert popularity to numeric (handle errors)
    song_df["popularity"] = pd.to_numeric(song_df["popularity"], errors="coerce")


    stream_df["hour"] = pd.to_datetime(stream_df["listen_time"]).dt.hour

    merged_df = stream_df.merge(song_df, on="track_id", how="left")
    merged_df["engagement_score"] = merged_df["duration_ms"] * merged_df["popularity"]/100

    hourly_kpis = merged_df.groupby("hour").agg(
        unique_listeners=("user_id", pd.Series.nunique),
        top_artist_per_hour=("artists", lambda x: x.value_counts().idxmax()),
        track_diversity_index=("track_id", lambda x: x.nunique() / len(x))
    ).reset_index()
    
    return hourly_kpis
