import glob
import pandas as pd
import os
import warnings
from transformations import phonepe_revenue_schema,transform_phonepe_revenue_df,phonepe_revenue_required_columns

warnings.filterwarnings('ignore')

phonepe_revenue_path = 'finance/csv_files/phonepe/Revenue'


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


## PHONEPE REVENUE:
# Use glob to get all CSV files in the folder
phonepe_revenue_csv_files = glob.glob(os.path.join(phonepe_revenue_path, "*.csv"))

allowed_files = [os.path.join(phonepe_revenue_path,"ARTHVITONLINE_FORWARD_TRANSACTION_JULY_2025.csv"),
                os.path.join(phonepe_revenue_path,"GOODSCOREONLINE_FORWARD_TRANSACTION_JULY_2025.csv"),
                os.path.join(phonepe_revenue_path,"GOODSCOREPGONLINE_FORWARD_TRANSACTION_JULY_2025.csv"),
                os.path.join(phonepe_revenue_path,"RUPICARDONLINE_FORWARD_TRANSACTION_JULY_2025.csv")]

# allowed_files = []

if len(allowed_files) > 0:
    phonepe_revenue_csv_files = allowed_files
# Loop through all the CSV files and read them into pandas DataFrames
for file in phonepe_revenue_csv_files:
    print(f"Processing file: {file}")

    df = pd.read_csv(file)

    new_columns = clean_column_names(df.columns)
    df.columns = new_columns

    for col in phonepe_revenue_required_columns:
        if col not in list(df.columns):
            df[col] = None

    df = df[phonepe_revenue_required_columns]
    df = transform_phonepe_revenue_df(df)

    df['file_name'] = file.split('/')[-1]

    df.to_gbq("finance_dataset.phonepe_revenue",project_id="rupiseva",if_exists="append",table_schema=phonepe_revenue_schema,chunksize=500)
    print(f"Processed file: {file}")
