import os
from google.cloud import storage
import pandas as pd
import io
import json
import pymssql

def process_csv_gcs(request):
    client = storage.Client()
    bucket_name = "linnworks-processed-orders"
    folder_prefix = "processed-orders/processed_orders/"
    bucket = client.bucket(bucket_name)

    blobs = list(client.list_blobs(bucket, prefix=folder_prefix))
    if not blobs:
        return "No CSV files found in the folder."

    latest_blob = max(blobs, key=lambda b: b.updated)
    data = latest_blob.download_as_bytes()

    df = pd.read_csv(io.BytesIO(data), sep=',', quotechar='"')
    df.columns = df.columns.str.strip()

    # Parse _airbyte_data JSON
    if '_airbyte_data' not in df.columns:
        return "_airbyte_data column not found in CSV."
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

        for row in df_combined.itertuples(index=False):
            cursor.execute(insert_sql, (row.pkOrderID, row.dProcessedOn, row.dReceivedDate))

        conn.commit()
        cursor.close()
        conn.close()

        return f"Inserted {len(df_combined)} rows into SQL Server successfully."

    except Exception as e:
        return f"An error occurred: {str(e)}"
