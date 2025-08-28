import glob
import pandas as pd
import os
import warnings
from transformations import phonepe_refund_csv_files_schema,transform_phonepe_refund_df

warnings.filterwarnings('ignore')

phonepe_refund_path = 'finance/csv_files/phonepe/Refund'


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





## PHONEPE REFUND:
# Use glob to get all CSV files in the folder
phonepe_refund_csv_files = glob.glob(os.path.join(phonepe_refund_path, "*.csv"))

## Add file names here
allowed_files = [os.path.join(phonepe_refund_path,"ARTHVITONLINE_REFUND_TRANSACTION_JULY_2025.csv"),
                 os.path.join(phonepe_refund_path,"GOODSCOREONLINE_REFUND_TRANSACTION_JULY_2025.csv"),
                 os.path.join(phonepe_refund_path,"GOODSCOREPGONLINE_REFUND_TRANSACTION_JULY_2025.csv"),
                 os.path.join(phonepe_refund_path,"RUPICARDONLINE_REFUND_TRANSACTION_JULY_2025.csv")]

# allowed_files = []

if len(allowed_files) > 0:
    phonepe_refund_csv_files = allowed_files

# Loop through all the CSV files and read them into pandas DataFrames
for file in phonepe_refund_csv_files:
    print(f"Processing file: {file}")

    df = pd.read_csv(file)
    new_columns = clean_column_names(df.columns)
    df.columns = new_columns

    df = transform_phonepe_refund_df(df)

    df['file_name'] = file.split('/')[-1]
    df.to_gbq("finance_dataset.phonepe_refund",table_schema=phonepe_refund_csv_files_schema,project_id="rupiseva",if_exists="append",chunksize=500)
