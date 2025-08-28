import pandas as pd

def transform_phonepe_refund_df(df):
    # Convert amount columns to float
    amount_columns = [
        'transaction_amount', 'total_refund_amount', 'offer_adjustment',
        'upi_amount', 'wallet_amount', 'credit_card_amount',
        'debit_card_amount', 'egv_amount'
    ]
    for col in amount_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')  # 'coerce' will turn invalid entries to NaN

    # Convert 'transaction_date' column to timestamp
    df['transaction_date'] = pd.to_datetime(df['transaction_date'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

    # Ensure other columns are strings
    string_columns = [
        'merchant_id', 'transaction_type', 'reversal_category', 'merchant_order_id', 
        'merchant_reference_id', 'phonepe_reference_id', 'transaction_status', 
        'forward_merchant_transaction_id', 'forward_transaction_reference_id',
        'arn', 'store_id', 'terminal_id', 'transaction_utr'
    ]
    for col in string_columns:
        df[col] = df[col].astype(str)

    return df

phonepe_revenue_required_columns = [
    "merchant_id","transaction_type","merchant_order_id",
    "merchant_reference_id","phonepe_reference_id","transaction_utr",
    "total_transaction_amount","transaction_date","transaction_status",
    "upi_amount","wallet_amount","credit_card_amount","debit_card_amount",
    "external_wallet_amount","egv_amount","store_id","terminal_id",
    "store_name","terminal_name","instrument","phonepe_attempt_reference_id",
    "phonepe_transaction_reference_id"
]

def transform_phonepe_revenue_df(df):
    # Convert amount columns to float
    amount_columns = [
        'egv_amount','external_wallet_amount','credit_card_amount','upi_amount',
        'debit_card_amount','total_transaction_amount','wallet_amount'
    ]
    for col in amount_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')  # 'coerce' will turn invalid entries to NaN

    # Convert 'transaction_date' column to timestamp
    df['transaction_date'] = pd.to_datetime(df['transaction_date'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

    # Ensure other columns are strings
    string_columns = [
        'merchant_order_id','store_name','store_id','phonepe_attempt_reference_id',
        'transaction_status','instrument','merchant_reference_id','terminal_name',
        'transaction_utr','phonepe_transaction_reference_id','merchant_id',
        'phonepe_reference_id','terminal_id'
    ]
    for col in string_columns:
        df[col] = df[col].astype(str)

    return df


def transform_phonepe_settlement_df(df):
    # Convert amount columns to float
    amount_columns = [
        'amount','fee','igst','cgst','sgst'
    ]
    for col in amount_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')  # 'coerce' will turn invalid entries to NaN

    # Convert 'creationdate', 'transactiondate', and 'settlementdate' columns to timestamp
    df['creationdate'] = pd.to_datetime(df['creationdate'], format='%d-%m-%Y', errors='coerce')
    df['transactiondate'] = pd.to_datetime(df['transactiondate'], format='%d-%m-%Y', errors='coerce')
    df['settlementdate'] = pd.to_datetime(df['settlementdate'], format='%d-%m-%Y', errors='coerce')


    # Ensure other columns are strings
    string_columns = [
        'paymenttype','merchantreferenceid','phonepereferenceid','from','instrument',
        'flow_type','bankreferenceno']
    for col in string_columns:
        df[col] = df[col].astype(str)

    return df


def transform_razorpay_df(df):
    # Convert amount columns to float
    amount_columns = [
        'debit','credit','amount','fee','tax','on_hold','settled'
    ]
    for col in amount_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')  # 'coerce' will turn invalid entries to NaN

    # Convert 'creationdate', 'transactiondate', and 'settlementdate' columns to timestamp
    df['created_at'] = pd.to_datetime(df['created_at'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
    df['settled_at'] = pd.to_datetime(df['settled_at'], format='%d-%m-%Y', errors='coerce')


    # Ensure other columns are strings
    string_columns = [
        'entity_id','type','currency','settlement_id','description','notes',
        'payment_id','arn','settlement_utr','order_id','order_receipt','method',
        'upi_flow','card_network','card_issuer','card_type','dispute_id','additional_utr']
    
    for col in string_columns:
        if col not in df.columns:
            df[col] = ''
        df[col] = df[col].astype(str)

    return df



def transform_payu_df(df):
    # Convert amount columns to float
    amount_columns = [
        'amount'
    ]
    for col in amount_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')  # 'coerce' will turn invalid entries to NaN

    # Convert 'creationdate', 'transactiondate', and 'settlementdate' columns to timestamp
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d %H:%M:%S', errors='coerce')


    # Ensure other columns are strings
    string_columns = [
        'transaction_id','activitytype','transaction_type','remarks']
    
    for col in string_columns:
        df[col] = df[col].astype(str)

    return df



# Example usage:
phonepe_refund_csv_files_schema = [
    {"name": "merchant_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "transaction_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "reversal_category", "type": "STRING", "mode": "NULLABLE"},
    {"name": "merchant_order_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "merchant_reference_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "phonepe_reference_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "transaction_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "total_refund_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "offer_adjustment", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "transaction_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "transaction_status", "type": "STRING", "mode": "NULLABLE"},
    {"name": "upi_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "wallet_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "credit_card_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "debit_card_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "egv_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "forward_merchant_transaction_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "forward_transaction_reference_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "arn", "type": "STRING", "mode": "NULLABLE"},
    {"name": "store_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "terminal_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "transaction_utr", "type": "STRING", "mode": "NULLABLE"},
    {"name": "file_name", "type": "STRING", "mode": "NULLABLE"}
]


phonepe_revenue_schema = [
    {"name": "merchant_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "transaction_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "merchant_order_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "merchant_reference_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "phonepe_reference_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "total_transaction_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "upi_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "wallet_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "credit_card_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "debit_card_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "external_wallet_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "egv_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "transaction_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "transaction_utr", "type": "STRING", "mode": "NULLABLE"},
    {"name": "transaction_status", "type": "STRING", "mode": "NULLABLE"},
    {"name": "store_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "terminal_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "store_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "terminal_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "instrument", "type": "STRING", "mode": "NULLABLE"},
    {"name": "phonepe_attempt_reference_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "phonepe_transaction_reference_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "file_name", "type": "STRING", "mode": "NULLABLE"}
]

phonepe_settlement_schema = [
    {"name": "paymenttype", "type": "STRING", "mode": "NULLABLE"},
    {"name": "merchantreferenceid", "type": "STRING", "mode": "NULLABLE"},
    {"name": "phonepereferenceid", "type": "STRING", "mode": "NULLABLE"},
    {"name": "from", "type": "STRING", "mode": "NULLABLE"},
    {"name": "instrument", "type": "STRING", "mode": "NULLABLE"},
    {"name": "flow_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "bankreferenceno", "type": "STRING", "mode": "NULLABLE"},
    {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "fee", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "igst", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "cgst", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "sgst", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "creationdate", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "transactiondate", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "settlementdate", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "file_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "subfolder_name", "type": "STRING", "mode": "NULLABLE"}    
]


razorpay_schema = [
    {"name": "debit", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "credit", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "fee", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "tax", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "on_hold", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "settled", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "settled_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "entity_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "currency", "type": "STRING", "mode": "NULLABLE"},
    {"name": "settlement_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "description", "type": "STRING", "mode": "NULLABLE"},
    {"name": "notes", "type": "STRING", "mode": "NULLABLE"},
    {"name": "payment_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "arn", "type": "STRING", "mode": "NULLABLE"},
    {"name": "settlement_utr", "type": "STRING", "mode": "NULLABLE"},
    {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "order_receipt", "type": "STRING", "mode": "NULLABLE"},
    {"name": "method", "type": "STRING", "mode": "NULLABLE"},
    {"name": "upi_flow", "type": "STRING", "mode": "NULLABLE"},
    {"name": "card_network", "type": "STRING", "mode": "NULLABLE"},
    {"name": "card_issuer", "type": "STRING", "mode": "NULLABLE"},
    {"name": "card_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "dispute_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "file_name", "type": "STRING", "mode": "NULLABLE"}
]

payu_schema = [
    {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "date", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "transaction_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "activitytype", "type": "STRING", "mode": "NULLABLE"},
    {"name": "transaction_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "remarks", "type": "STRING", "mode": "NULLABLE"},
    {"name": "subfolder_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "file_name", "type": "STRING", "mode": "NULLABLE"}
]
