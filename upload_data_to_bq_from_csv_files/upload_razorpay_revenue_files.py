import glob
import pandas as pd
import os
import warnings
from transformations import transform_razorpay_df,razorpay_schema

warnings.filterwarnings('ignore')

razorpay_path = 'finance/csv_files/razorpay'


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


## RAZORPAY REVENUE:
# Use glob to get all CSV files in the folder
razorpay_csv_files = glob.glob(os.path.join(razorpay_path, "*.csv"))
# Loop through all the CSV files and read them into pandas DataFrames

allowed_files = [os.path.join(razorpay_path,"transactions-2 - Jul 25.csv"),os.path.join(razorpay_path,"transactions - Jul 25.csv")]
# allowed_files = []

if len(allowed_files) > 0:
    razorpay_csv_files = allowed_files

for file in razorpay_csv_files:
    print(f"Processing file: {file}")

    df = pd.read_csv(file)

    new_columns = clean_column_names(df.columns)
    df.columns = new_columns

    df = transform_razorpay_df(df)

    df['file_name'] = file.split('/')[-1]
    df.to_gbq("finance_dataset.razorpay",project_id="rupiseva",if_exists="append",table_schema=razorpay_schema,chunksize=500)
    print(f"Processed file: {file}")