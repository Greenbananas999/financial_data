import requests
import os
from dotenv import load_dotenv
import json
import pandas as pd
import re
from sqlalchemy import create_engine, text
import utils

# Load environment variables
load_dotenv()
FRED_API_KEY = os.getenv("FRED_API_KEY")

def fred_request(series_id, data_type):
    """
    Make a request to FRED API for series data.
    
    Args:
        series_id (str): The FRED series identifier
        
    Returns:
        dict: JSON response from the FRED API
    """
    base_url = f"https://api.stlouisfed.org/fred/{data_type}"
    
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json"
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise an error for bad status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        return None
    
        
def update_fred_data(series_id_list, update_type):
    
    if update_type == "new":
        table_name = "fred_series"
        check_df = utils.get_table_from_db(table_name)
        try:
            id_skips = check_df["id"].tolist()
        except:
            id_skips = ""
        print(id_skips)
        
        for series_id in series_id_list:
            if series_id not in id_skips:
                get_fred_data(series_id, data_type_list, main_data_type)
    
    elif update_type == "update":
        utils.drop_tables(main_data_type, data_type_list)    
        for series_id in series_id_list:
            get_fred_data(series_id, data_type_list, main_data_type)
    

def get_fred_data(series_id, data_type_list, main_data_type):
    data = fred_request(series_id, main_data_type)
    data_name = re.sub(r'[^a-zA-Z0-9]', '_', data[f"{main_data_type}s"][0]["title"])
    parent_series_id = data[f"{main_data_type}s"][0]["id"]
    data_name = re.sub(r'_+', '_', data_name)
    data_name = data_name.strip('_')
    
    df = pd.DataFrame(data[f"{main_data_type}s"])
    utils.save_json_to_file(data, f"./data/FRED_JSON/{main_data_type}/{data_name}_fred_{series_id}_{main_data_type}_data.json")
    df.to_csv(f"./data/FRED_CSV/{main_data_type}/{data_name}_fred_{series_id}_{main_data_type}_data.csv", index=False)
    utils.df_to_sql(df, f"fred_{main_data_type}")
    
    for data_type in data_type_list:
        full_data_type = f"{main_data_type}/{data_type}"
        data = fred_request(series_id, full_data_type)
        df = pd.DataFrame(data[f"{data_type}"])
        utils.save_json_to_file(data, f"./data/FRED_JSON/{data_type}/{data_name}_fred_{series_id}_{main_data_type}_{data_type}_data.json")
        df.to_csv(f"./data/FRED_CSV/{data_type}/{data_name}_fred_{series_id}_{main_data_type}_{data_type}_data.csv", index=False)
        if data_type == "observations":
            df["series_id"] = parent_series_id
            utils.df_to_sql(df, f"fred_{main_data_type}_{data_type}")
        else:
            utils.df_to_sql(df, f"fred_{main_data_type}_{data_type}")
            


if __name__ == "__main__":
    
    if 1==1:
        series_id_list= ["GNPCA", "APU0000708111", "APU0000703112", "APU000074714", "WPS012202", "FEDFUNDS", "TTLCONS", "TLMFGCONS", "POILBREUSDM", "POLVOILUSDM", "PIORECRUSDM", "PWHEAMTUSDM", "PNGASEUUSDM"]
        main_data_type = "series"
        data_type_list= ["observations", "categories"]

        update_type = "new"
        update_fred_data(series_id_list, update_type)
        
        
    
    
