from sqlalchemy import create_engine
import pandas as pd

def fetch_users_and_tracks(rds_host, rds_port, db_name, username, password):
    """
    Fetch the users and tracks tables from a PostgreSQL RDS database and return them as a list of DataFrames.

    :param rds_host: str - The RDS PostgreSQL host endpoint
    :param rds_port: int - The database port (default: 5432)
    :param db_name: str - The name of the database
    :param username: str - The database username
    :param password: str - The database password
    :return: list - A list containing two DataFrames [users_df, tracks_df]
    """
    try:
        # Create the database connection string
        engine = create_engine(f'postgresql://{username}:{password}@{rds_host}:{rds_port}/{db_name}')
        
        # Define the table names
        tables = ["users", "tracks"]
        dataframes = []

        # Fetch each table into a DataFrame
        with engine.connect() as connection:
            for table in tables:
                df = pd.read_sql(f"SELECT * FROM {table};", connection)
                dataframes.append(df)

        return dataframes  # [users_df, tracks_df]

    except Exception as e:
        print(f"Error fetching data: {e}")
        return [pd.DataFrame(), pd.DataFrame()]    

  
