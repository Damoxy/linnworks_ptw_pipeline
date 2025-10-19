import os
from google.cloud import storage
import pandas as pd
import io
import json
import pymssql

BATCH_SIZE = 500 

def process_csv_gcs(event, context):
    """Background Cloud Function to process CSV from GCS."""
    
    bucket_name = event['bucket']
    blob_name = event['name']

    if not blob_name.endswith(".csv"):
        print(f"Ignoring non-CSV file: {blob_name}")
        return

    print(f"Processing file: {blob_name} from bucket: {bucket_name}")

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data = blob.download_as_bytes()

    df = pd.read_csv(io.BytesIO(data), sep=',', quotechar='"')
    df.columns = df.columns.str.strip()

    if '_airbyte_data' not in df.columns:
        print("_airbyte_data column not found in CSV.")
        return

    # Parse JSON column
    df_json = df['_airbyte_data'].apply(json.loads).apply(pd.Series)
    df_combined = pd.concat([df, df_json], axis=1)

    SQL_SERVER = os.getenv("SQL_SERVER")
    SQL_USER = os.getenv("SQL_USER")
    SQL_PASSWORD = os.getenv("SQL_PASSWORD")
    SQL_DATABASE = os.getenv("SQL_DATABASE")

    try:
        conn = pymssql.connect(server=SQL_SERVER,
                               user=SQL_USER,
                               password=SQL_PASSWORD,
                               database=SQL_DATABASE)
        cursor = conn.cursor()

        cursor.execute("DELETE FROM [linnworks].[staging].[processed_orders]")
        conn.commit()

        insert_sql = """
        INSERT INTO [linnworks].[staging].[processed_orders]
        (pkOrderID, dProcessedOn, dReceivedDate)
        VALUES (%s, %s, %s)
        """

        total_rows = len(df_combined)
        for start in range(0, total_rows, BATCH_SIZE):
            batch = df_combined.iloc[start:start + BATCH_SIZE]
            batch = batch.where(pd.notnull(batch), None)
            values = batch[['pkOrderID','dProcessedOn','dReceivedDate']].values.tolist()
            cursor.executemany(insert_sql, values)
            conn.commit()
            print(f"Inserted batch {start}-{start + len(values)-1}")

        cursor.close()
        conn.close()
        print(f"Inserted total {total_rows} rows successfully.")

    except Exception as e:
        print(f"Error inserting into SQL Server: {str(e)}")
