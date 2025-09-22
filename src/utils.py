import os
from dotenv import load_dotenv
import json
import pandas as pd
import re
from sqlalchemy import create_engine, text

load_dotenv()
DB_CONNECTION = os.getenv("DB_CONNECTION")  # Add this to your .env file

def get_table_from_db(table_name: str, params: str = "1=1", schema: str = None) -> pd.DataFrame:
    try:
        # Create database engine
        engine = create_engine(DB_CONNECTION)
        # Construct query
        query = f"SELECT * FROM {schema+'.' if schema else ''}{table_name} WHERE {params}"
        # Read data into DataFrame
        df = pd.read_sql(query, engine)
        return df
        
    except Exception as e:
        print(f"Error retrieving table {table_name}: {e}")
        return pd.DataFrame()
    
def drop_tables(main_data_type, data_type_list):
    engine = create_engine(DB_CONNECTION)
    with engine.connect() as connection:
        connection.execute(text(f"DROP TABLE IF EXISTS fred_{main_data_type}"))
        for data_type in data_type_list:
            connection.execute(text(f"DROP TABLE IF EXISTS fred_{main_data_type}_{data_type}"))
            
def save_json_to_file(json_data, filename):
    """
    Save JSON data to a file.
    
    Args:
        json_data (dict): JSON data to save
        filename (str): Output file path
    """
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2)
    print(f"JSON data saved to {filename}")
    
def get_table_from_db(table_name: str, params: str = "1=1", schema: str = None) -> pd.DataFrame:
    try:
        # Create database engine
        engine = create_engine(DB_CONNECTION)
        # Construct query
        query = f"SELECT * FROM {schema+'.' if schema else ''}{table_name} WHERE {params}"
        # Read data into DataFrame
        df = pd.read_sql(query, engine)
        return df
        
    except Exception as e:
        print(f"Error retrieving table {table_name}: {e}")
        return pd.DataFrame()
    
def df_to_sql(df, table_name, schema=None):
    """
    Push DataFrame to SQL database.
    
    Args:
        df (pandas.DataFrame): DataFrame to save
        table_name (str): Name of the table in database
        schema (str, optional): Database schema name
    """
    try:
        engine = create_engine(DB_CONNECTION)
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='append',  # Options: 'fail', 'replace', 'append'
            index=False,
            method='multi'
        )
        print(f"Successfully saved {table_name} to database")
    except Exception as e:
        print(f"Error saving to database: {e}")