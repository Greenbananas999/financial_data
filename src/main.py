import os
import json
from dotenv import load_dotenv
import pandas as pd
from requests_handler import income_statement_request

#https://twelvedata.com/docs

load_dotenv()  # Loads environment variables from .env

APIKEY = os.getenv("APIKEY")

def save_json_to_file(json_data, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2)
    print(f"JSON data saved to {filename}")      

def convert_to_millions(value):
    """
    Converts large numeric values to millions if applicable.
    """
    if isinstance(value, (int, float)) and abs(value) > 1e6:
        return round(value / 1e6, 2)
    return value

def process_income_statement(symbol_list=["AAPL"]):
    """
    Process income statements for multiple symbols and combine into a single DataFrame
    
    Args:
        symbol_list (list): List of stock symbols to process
        
    Returns:
        pandas.DataFrame: Combined DataFrame with all symbols and years
    """
    all_data = []
    
    for symbol in symbol_list:
        print(f"Processing {symbol}...")
        data = income_statement_request(symbol)
        
        if "income_statement" in data and isinstance(data["income_statement"], list):
            statements = data["income_statement"]
            for statement in statements:
                # Create a copy of the flattened and converted dictionary
                flat_dict = flatten_dict(statement)
                converted_dict = {k: convert_to_millions(v) for k, v in flat_dict.items()}
                
                # Add symbol identifier
                converted_dict['symbol'] = symbol
                converted_dict['year'] = statement.get('year', 'Unknown')
                
                all_data.append(converted_dict)
    
    # Create DataFrame from all collected data
    if all_data:
        df = pd.DataFrame(all_data)
        # Set multi-index with symbol and year
        df.set_index(['symbol', 'year'], inplace=True)
        df.sort_index(inplace=True)
    else:
        df = pd.DataFrame()
        
    print("\nProcessed DataFrame:")
    print(df)
    return df

def flatten_dict(nested_dict, prefix=''):
    """
    Recursively flattens a nested dictionary using dot notation for nested keys.
    """
    flat_dict = {}
    for key, value in nested_dict.items():
        new_key = f"{prefix}.{key}" if prefix else key
        
        if isinstance(value, dict):
            flat_dict.update(flatten_dict(value, new_key))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    flat_dict.update(flatten_dict(item, f"{new_key}_{i}"))
                else:
                    flat_dict[f"{new_key}_{i}"] = item
        else:
            flat_dict[new_key] = value
            
    return flat_dict

# Example usage:
with open('./data/income_statement_data.json') as f:
    nested_dict = json.load(f)
flat_dic = flatten_dict(nested_dict)
save_json_to_file(flat_dic, "./data/flat.json")

if __name__ == "__main__":
    symbol_list = ["AAPL", "MSFT", "NVDA"]
    df = process_income_statement(symbol_list)
    df.to_csv("./data/df_output.csv")

