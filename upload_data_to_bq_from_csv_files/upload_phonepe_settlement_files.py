import glob
import pandas as pd
import os
import warnings
from transformations import transform_phonepe_settlement_df,phonepe_settlement_schema

warnings.filterwarnings('ignore')

phonepe_settlement_path = 'finance/csv_files/phonepe/Settlement'


def clean_column_names(column_list):
    """
    Clean column names to follow a consistent format.
    """
    new_column_list = []
    for column in column_list:
        cleaned_column = column.replace(")", " ")
        cleaned_column = cleaned_column.replace("(", " ")
        cleaned_column = cleaned_column.replace(":", " ")
        cleaned_column = cleaned_column.replace("%", "perc")
        cleaned_column = cleaned_column.strip()
        cleaned_column = cleaned_column.replace("  ", " ")
        cleaned_column = cleaned_column.replace(" ", "_")
        cleaned_column = cleaned_column.lower()
        new_column_list.append(cleaned_column)
    return new_column_list


## PHONEPE SETTLEMENT:
# Use glob to get all CSV files in the folder
phonepe_settlement_csv_files = glob.glob(os.path.join(phonepe_settlement_path, '**', '*.csv'), recursive=True)
# Loop through all the CSV files and read them into pandas DataFrames
for file in phonepe_settlement_csv_files:
    df = pd.read_csv(file)

    new_columns = clean_column_names(df.columns)
    df.columns = new_columns

    df = transform_phonepe_settlement_df(df)

    # Optionally, you could add a column to capture the subfolder name
    subfolder_name = file.split(os.path.sep)[-2]  # Extract the immediate subfolder name
    df['subfolder_name'] = subfolder_name
    df['file_name'] = file.split('/')[-1]
    
    df.to_gbq("finance_dataset.phonepe_settlement",project_id="rupiseva",if_exists="append",table_schema=phonepe_settlement_schema,chunksize=500)
    print(f"Processed file: {file}")
