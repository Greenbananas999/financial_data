import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import utils

# Load environment variables
load_dotenv()
DB_CONNECTION = os.getenv("DB_CONNECTION")

# Example usage
if __name__ == "__main__":
    check_ids = ['APU000074714', 'WPS012202']
    params_list = [f"series_id= '{check_ids[0]}'", f"series_id= '{check_ids[1]}'"]
    dataframes = []  # List to store DataFrames

    for params in params_list:
        df = utils.get_table_from_db(table_name="fred_series_observations", params=params)

        if not df.empty:
            # Rename value column to include series_id to avoid conflicts
            series_id = params.split("'")[1]  # Extract series_id from params
            df = df.rename(columns={'value': f'value_{series_id}'})
            # Remove rows where value equals '.'
            value_col = f'value_{series_id}'
            df = df[df[value_col] != '.']
            dataframes.append(df)
            print(f"\nData for {params}:")
            print(df.head())

    if len(dataframes) == 2:
        # Join DataFrames on date field
        merged_df = pd.merge(
            dataframes[0],
            dataframes[1],
            on='date',
            how='inner',
            suffixes=('_df1', '_df2')
        )

        # Sort by date
        merged_df = merged_df.sort_values('date')

        # Select only the date and value columns for both series
        merged_df = merged_df[["date", f"value_{check_ids[0]}", f"value_{check_ids[1]}"]]
        corr = merged_df[f"value_{check_ids[0]}"].corr(merged_df[f"value_{check_ids[1]}"])
        print(f"----------{corr}----------")

        # Save to CSV for verification
        merged_df.to_csv('./data/merged_observations.csv', index=False)
    else:
        print("\nNeed exactly 2 DataFrames to merge")