# Music Streaming KPI Pipeline

This repository contains an Apache Airflow pipeline that extracts data from an Amazon RDS database and an S3 bucket, processes the data, and loads it into Amazon Redshift. The pipeline is orchestrated using AWS Managed Workflows for Apache Airflow (MWAA).

## Table of Contents
- [Architecture Overview](#architecture-overview)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
- [Airflow DAG Configuration](#airflow-dag-configuration)
- [Validation Checks](#validation-checks)
- [Error Handling](#error-handling)
- [Conclusion](#conclusion)

---

## Architecture Overview

The pipeline follows these steps:
1. Extracts user and track metadata from an Amazon RDS PostgreSQL database.
2. Extracts streaming data from CSV files stored in an Amazon S3 bucket.
3. Cleans and processes the data.
4. Computes KPIs for music streams.
5. Loads processed KPIs into an Amazon Redshift cluster for analysis.

### Data Sources:
- **Amazon RDS PostgreSQL**: Stores user and track metadata.
- **Amazon S3**: Contains raw streaming data as CSV files.
- **Amazon Redshift**: Stores transformed data for analytics.

---

## Prerequisites
Before setting up the pipeline, ensure you have the following:

- **AWS Account** with necessary IAM permissions.
- **Amazon MWAA Environment** for Airflow orchestration.
- **Amazon RDS (PostgreSQL)** with relevant tables (`users` and `tracks`).
- **Amazon S3 Bucket** to store raw and processed data.
- **Amazon Redshift Cluster** for analytics.

---

## Setup Instructions

### 1. Configure AWS Resources

#### Create an Amazon S3 Bucket
- Store raw streaming data under `s3://your-bucket-name/streaming-data/`
- Store processed data under `s3://your-bucket-name/stagingData/`

#### Set Up Amazon RDS PostgreSQL
Ensure the `users` and `tracks` tables exist:
```sql
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(255),
    created_at TIMESTAMP
);

CREATE TABLE tracks (
    track_id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    genre VARCHAR(100),
    duration FLOAT,
    artist VARCHAR(255)
);
```

#### Create Amazon Redshift Tables
```sql
CREATE TABLE kpis_genre (
    track_genre VARCHAR(100),
    listen_count INTEGER,
    average_track_duration DECIMAL(10,2),
    popularity_index DECIMAL(10,2),
    track_id VARCHAR(50)
);

CREATE TABLE kpis_hourly (
    hour INTEGER,
    unique_listeners INTEGER,
    top_artist_per_hour VARCHAR(255),
    track_diversity_index DECIMAL(10,6)
);
```

#### Set Up IAM Roles
- Ensure Redshift has the required `IAM_ROLE` for S3 access.
- Configure Airflow to use an AWS connection (`aws_default`).

---

## Airflow DAG Configuration

The DAG follows these steps:
1. **Extract Data** from RDS and S3.
2. **Transform Data** (cleaning and processing).
3. **Compute KPIs** (genre and hourly KPIs).
4. **Load Data** into Redshift.

Set the following Airflow Variables:
```bash
s3_bucket = "your-bucket-name"
bucket_prefix = "stagingData/"
iam_arn = "arn:aws:iam::your-account-id:role/RedshiftS3Access"
redshift_endpoint = "your-redshift-cluster-endpoint"
port = "5439"
database = "your-database"
user = "your-username"
password = "your-password"
```

---

## Validation Checks

The pipeline validates the following before proceeding:
1. **Data Completeness**: Ensures all required columns exist.
2. **Data Quality**: Checks for missing or inconsistent values.
3. **Data Integrity**: Verifies uniqueness and primary key constraints.

If any validation check fails, the DAG terminates execution.

---

## Error Handling
- **AWS Connection Errors**: Caught and logged.
- **Data Extraction Failures**: Stops execution if RDS/S3 data is missing.
- **Redshift Load Errors**: Ensures rollback in case of failure.

---

## Security Issues
-  **Tightening Access to Redshift**: Security groups configured to limit data from a specific ip for the Redshift instance
-  **Implementing Resource Specific Policies**: The principle of least priviledge was adhered to.

---

## Conclusion
This Airflow pipeline enables automated extraction, transformation, and loading (ETL) of music streaming data. It ensures reliable KPI computation and efficient storage in Redshift for analytics.

---

### Author
**Ebenezer Quayson**

