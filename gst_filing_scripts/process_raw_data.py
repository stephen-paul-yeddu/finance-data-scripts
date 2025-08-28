from google.cloud import bigquery
import csv

client = bigquery.Client()

raw_data_queries = ['queries/phonepe_refund_state_product_split.sql',
                    'queries/phonepe_transactions_state_product_split.sql',
                    'queries/razorpay_transactions_state_product_split.sql']


date_input = "'" + "2025-07-01" + "'"

for query_file in raw_data_queries:

    with open(query_file,'r') as sql_query_string:
        query = sql_query_string.read().replace('date_input',date_input)

    print(f"Processing File: {query_file}....")

    # Run the query and get the results in chunks
    query_job = client.query(query)
    # Get the column names from the schema
    columns = [field.name for field in query_job.result().schema]

    csv_filename = 'raw_data_output/' + query_file.split('/')[1].replace('.sql','_') + date_input.split("'")[1].replace('-','_').replace('_01','') + '.csv'
    

    # Open the CSV file in write mode
    with open(csv_filename, "w", newline="", encoding="utf-8") as file:
        # Create a CSV writer object
        writer = csv.writer(file)
        
        # Write the header row to the CSV
        writer.writerow(columns)
        
        # Process each row in the query result
        count = 0

        for row in query_job.result():  # Iterating through the query result rows
            data = []
            for col in columns:
                data.append(row[col])
            
            # Write the row data to the CSV file
            writer.writerow(data)

            count += 1
            if count % 10000 == 0:
                print(f"Processed rows: {count}")

    print(f"Download Complete! Data saved in {csv_filename}")
