import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import utils

# Load environment variables
load_dotenv()
DB_CONNECTION = os.getenv("DB_CONNECTION")

def compare_fred_data(id_1, id_2, offset_months=0):
    # Connect to the database
    engine = create_engine(DB_CONNECTION)

    # Fetch data (parameterized)
    q1 = text(f"SELECT date, value FROM fred_series_observations WHERE series_id = '{id_1}'")
    q2 = text(f"SELECT date, value FROM fred_series_observations WHERE series_id = '{id_2}'")

    df1 = pd.read_sql(q1, engine)
    df2 = pd.read_sql(q2, engine)

    if df1.empty or df2.empty:
        print("One of the DataFrames is empty. Please check the series IDs.")
        return

    # Ensure datetime for merge key
    df1["date"] = pd.to_datetime(df1["date"])
    df2["date"] = pd.to_datetime(df2["date"])

    # Rename value columns to avoid conflicts
    df1 = df1.rename(columns={'value': f'value_{id_1}'})
    df2 = df2.rename(columns={'value': f'value_{id_2}'})

    # Remove rows where value equals '.'
    df1 = df1[df1[f'value_{id_1}'] != '.']
    df2 = df2[df2[f'value_{id_2}'] != '.']

    # Convert to numeric
    df1[f'value_{id_1}'] = pd.to_numeric(df1[f'value_{id_1}'], errors='coerce')
    df2[f'value_{id_2}'] = pd.to_numeric(df2[f'value_{id_2}'], errors='coerce')
    df1 = df1.dropna(subset=[f'value_{id_1}'])
    df2 = df2.dropna(subset=[f'value_{id_2}'])

    # Apply month offset to df2 values (keeps dates aligned correctly)
    df2 = move_values_up_one_month(
        df2,
        date_col="date",
        columns=[f"value_{id_2}"],
        months=offset_months,
        keep_original=False
    )
    # Re-ensure datetime after transformation
    df2["date"] = pd.to_datetime(df2["date"])

    # Merge DataFrames on date field
    merged_df = pd.merge(
        df1,
        df2,
        on='date',
        how='inner',
        suffixes=('_df1', '_df2')
    )

    if merged_df.empty:
        print(f"No overlapping dates for {id_1} and {id_2} at offset {offset_months}.")
        return None

    # Sort by date
    merged_df = merged_df.sort_values('date')

    # Select only the date and value columns for both series
    merged_df = merged_df[["date", f"value_{id_1}", f"value_{id_2}"]]

    # Calculate correlation
    corr = merged_df[f"value_{id_1}"].corr(merged_df[f"value_{id_2}"])
    print(f"Correlation between {id_1} and {id_2} (offset {offset_months}): {corr}")

    return corr

def loop_all_fred_series():
    engine = create_engine(DB_CONNECTION)
    query = "SELECT id FROM fred_series"
    df = pd.read_sql(query, engine)
    series_ids = df['id'].tolist()

    corr_df = pd.DataFrame(columns=['Series_1', 'Series_2', 'Month_Shift', 'Correlation'])

    for i in range(len(series_ids)):
        for j in range(i + 1, len(series_ids)):
            for k in range(12):  # k=0 for same month, k=1 for next month
                corr = compare_fred_data(series_ids[i], series_ids[j], k)
                new_row = pd.DataFrame([{
                    'Series_1': series_ids[i],
                    'Series_2': series_ids[j],
                    'Month_Shift': k,
                    'Correlation': corr
                }])
                corr_df = pd.concat([corr_df, new_row], ignore_index=True)

    utils.df_to_sql(corr_df, 'fred_series_correlation', if_exists='replace')

def move_values_up_one_month(df: pd.DataFrame, date_col: str = "date", columns=None, months: int = 1, keep_original: bool = False, suffix: str = "_plus1m") -> pd.DataFrame:
    """
    Move values forward by one month for given columns, aligning each value to the next month's date.
    This shifts values via date arithmetic (not row shift), so it tolerates missing months.

    Args:
        df: Input DataFrame containing a date column and one or more value columns.
        date_col: Name of the date column.
        columns: List of columns to shift. If None, shifts all columns except date_col and 'series_id'.
        months: Number of months to move forward.
        keep_original: If True, keeps original columns and writes shifted values to new columns with suffix.
                       If False, replaces the specified columns with shifted values.
        suffix: Suffix used when keep_original=True.

    Returns:
        DataFrame with values aligned to date + months.
    """
    base = df.copy()
    base[date_col] = pd.to_datetime(base[date_col])

    if columns is None:
        columns = [c for c in base.columns if c not in {date_col, "series_id"}]

    # Build a shifted frame where values are aligned to date + months
    shifted = base[[date_col] + columns].copy()
    shifted[date_col] = shifted[date_col] + pd.DateOffset(months=months)

    if keep_original:
        rename_map = {c: f"{c}{suffix}" for c in columns}
        shifted = shifted.rename(columns=rename_map)
        merged = base.merge(shifted, on=date_col, how="left")
        return merged
    else:
        # Replace original columns with the shifted ones
        merged = base.drop(columns=columns).merge(shifted, on=date_col, how="left")
        return merged


# Example usage
if __name__ == "__main__":
    loop_all_fred_series()