import os
import pandas as pd
from pandas_gbq import to_gbq
import warnings

warnings.filterwarnings('ignore')

# Write DataFrame to BigQuery
project_id = 'rupiseva'  # Replace with your project ID
dataset_id = 'test_dataset'  # Replace with your BigQuery dataset
table_id = 'gupshup_data'  # Replace with your desired table name

csv_folder_path = 'gupshup_data_upload/csv_files'

csv_files = os.listdir(csv_folder_path)
schema = [
    {'name': 'phone_number', 'type': 'INTEGER'},
    {'name': 'sender', 'type': 'INTEGER'},
    {'name': 'transaction_id', 'type': 'INTEGER'},
    {'name': 'message_id', 'type': 'STRING'},
    {'name': 'consumption_id', 'type': 'INTEGER'},
    {'name': 'type', 'type': 'STRING'},
    {'name': 'sent', 'type': 'TIMESTAMP'},
    {'name': 'delivered', 'type': 'TIMESTAMP'},
    {'name': 'read', 'type': 'TIMESTAMP'},
    {'name': 'failed', 'type': 'TIMESTAMP'},
    {'name': 'status', 'type': 'STRING'},
    {'name': 'cause', 'type': 'STRING'},
    {'name': 'channel', 'type': 'STRING'},
    {'name': 'template_id', 'type': 'INTEGER'},
    {'name': 'number_messages', 'type': 'INTEGER'},
    {'name': 'delivery_code', 'type': 'STRING'},
    {'name': 'dlt_template_id', 'type': 'STRING'},
    {'name': 'principal_entity_id', 'type': 'STRING'},
    {'name': 'retry_status', 'type': 'STRING'},
    {'name': 'conversation_id', 'type': 'STRING'},
    {'name': 'category', 'type': 'STRING'},
    {'name': 'category_type', 'type': 'STRING'},
    {'name': 'extra', 'type': 'STRING'},
    {'name': 'pricing_category', 'type': 'STRING'},
    {'name': 'project_id', 'type': 'INTEGER'},
    {'name': 'org_id', 'type': 'INTEGER'},
    {'name': 'meta_error_code', 'type': 'STRING'},
    {'name': 'meta_error_message', 'type': 'STRING'},
    {'name': 'template_name', 'type': 'STRING'},
    {'name': 'template_language', 'type': 'STRING'},
    {'name': 'requested', 'type': 'STRING'},
    {'name': 'submitted', 'type': 'STRING'},
    {'name': 'button_click_timestamp', 'type': 'STRING'},
    {'name': 'button_name', 'type': 'STRING'},
    {'name': 'link_click_timestamp', 'type': 'STRING'},
    {'name': 'campaign_id', 'type': 'STRING'},
    {'name': 'file_name', 'type': 'STRING'},
    {'name': 'month', 'type': 'STRING'}
]

def clean_column_names(df):
    cleaned_columns = []
    for column in df.columns:
        cleaned_column = column.replace(' ','_').lower()
        cleaned_column = cleaned_column.replace('(','').lower()
        cleaned_column = cleaned_column.replace(')','').lower()
        cleaned_columns.append(cleaned_column)
    df.columns = cleaned_columns
    return df


column_names = ['phone_number','sender','transaction_id','message_id','consumption_id',
                'type','sent','delivered','read','failed','status','cause','channel',
                'template_id','number_messages','delivery_code','dlt_template_id',
                'principal_entity_id','retry_status','conversation_id','category',
                'category_type','extra','pricing_category','project_id','org_id',
                'meta_error_code','meta_error_message','template_name','template_language',
                'requested','submitted','button_click_timestamp','button_name','link_click_timestamp',
                'campaign_id','file_name','month']


int_columns = ['phone_number','sender','transaction_id','consumption_id',
               'template_id','number_messages','project_id','org_id']

timestamp_columns = ['sent','delivered','read','failed']


for file in csv_files:
    if file.endswith('.csv'):

        print(f"Processing File {os.path.join(csv_folder_path,file)}")
        df = pd.read_csv(os.path.join(csv_folder_path,file))

        df['file_name'] = os.path.join(csv_folder_path,file)
        df['month'] = file.split('-')[-1].split('.')[0]

        df = df[['PHONENO','SENDER','TRANSACTION_ID','MESSAGE_ID','CONSUMPTION_ID','TYPE','SENT','DELIVERED','READ','FAILED','STATUS','CAUSE','CHANNEL','TEMPLATE_ID','NUMBER_MESSAGES','DELIVERY_CODE','DLT_TEMPLATEID','PRINCIPAL_ENTITYID','RETRY_STATUS','CONVERSATION_ID','CATEGORY','CATEGORY_TYPE','EXTRA','PRICING_CATEGORY','PROJECT_ID','ORG_ID','META_ERROR_CODE','META_ERROR_MESSAGE','TEMPLATE_NAME','TEMPLATE_LANGUAGE','REQUESTED','SUBMITTED','BUTTON_CLICK_TIMESTAMP','BUTTON_NAME','LINK_CLICK_TIMESTAMP','CAMPAIGN_ID','file_name','month']]

        df.columns=column_names

        for column in df.columns:
            if df[column].name in timestamp_columns:
                df[column] = pd.to_datetime(df[column])
            
            elif df[column].name in int_columns:
                df[column] = df[column].astype(float)

            else:
                df[column] = df[column].astype(str).fillna('')

        # Write to BigQuery
        to_gbq(df, destination_table=f'{dataset_id}.{table_id}', project_id=project_id, if_exists='append',table_schema=schema)
        print(f"Written {file} to {dataset_id}.{table_id}")
