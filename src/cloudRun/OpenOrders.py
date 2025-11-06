import os
import requests
import pymssql
import pandas as pd
import json
import math
import time
import logging
from datetime import datetime, timezone
from flask import Request

logging.basicConfig(level=logging.INFO)

# ------------------------------
# CONFIG
# ------------------------------
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DATABASE = os.getenv("SQL_DATABASE")

LINNWORKS_APP_ID = os.getenv("LINNWORKS_APP_ID")
LINNWORKS_APP_SECRET = os.getenv("LINNWORKS_APP_SECRET")
LINNWORKS_TOKEN = os.getenv("LINNWORKS_TOKEN")

# ------------------------------
# Helper 
# ------------------------------
def safe_date(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None

def safe_str(value):
    if value is None:
        return None
    return str(value).strip()

# ------------------------------
# Linnworks API 
# ------------------------------
def get_linnworks_access_token():
    url = "https://api.linnworks.net/api/Auth/AuthorizeByApplication"
    payload = {
        "ApplicationId": LINNWORKS_APP_ID,
        "ApplicationSecret": LINNWORKS_APP_SECRET,
        "Token": LINNWORKS_TOKEN
    }
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        data = response.json()
        token = data.get("Token") or data.get("AccessToken")
        if not token:
            logging.error("Access token not found in response: %s", data)
            return None
        logging.info("Fetched Linnworks access token successfully.")
        return token
    except Exception as e:
        logging.error("Error fetching Linnworks access token: %s", e)
        return None

# ------------------------------
# Airbyte Check
# ------------------------------
def airbyte_completed_today(cursor):
    cursor.execute("""
        SELECT MAX(_airbyte_extracted_at)
        FROM [linnworks].[staging].[_airbyte_raw_processed_order_details]
    """)
    last_run_ms = cursor.fetchone()[0]
    if not last_run_ms:
        return False, None
    last_run_dt = datetime.fromtimestamp(last_run_ms / 1000, tz=timezone.utc)
    today = datetime.now(timezone.utc).date()
    return last_run_dt.date() == today, last_run_dt

# ------------------------------
# Cleanup old unprocessed records
# ------------------------------
def clear_unprocessed_orders(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM [linnworks].[staging].[_airbyte_raw_processed_order_details]
            WHERE Processed = 0
        """)
        affected = cursor.rowcount
        conn.commit()
        cursor.close()
        logging.info("Cleared %d old unprocessed records before inserting new ones.", affected)
    except Exception as e:
        logging.error("Error clearing old unprocessed records: %s", e)
        conn.rollback()

# ------------------------------
# Fetch Open Orders
# ------------------------------
def fetch_open_orders(cursor, access_token):
    cursor.execute("SELECT DISTINCT pkStockLocationId FROM [linnworks].[lw].[StockLocation]")
    location_ids = [row[0] for row in cursor.fetchall()]
    logging.info("Found %d locations", len(location_ids))

    url_open_orders = "https://eu-ext.linnworks.net/api/OpenOrders/GetOpenOrderIds"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": access_token
    }

    open_order_ids = []
    for location_id in location_ids:
        payload = {
            "LocationId": str(location_id),
            "ViewId": 1,
            "EntriesPerPage": 100000
        }
        try:
            response = requests.post(url_open_orders, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json().get("Data", [])
            open_order_ids.extend(data)
            logging.info("Fetched %d orders for location %s", len(data), location_id)
        except Exception as e:
            logging.error("Error fetching orders for %s: %s", location_id, e)

    # Fetch order details in batches
    url_order_details = "https://eu-ext.linnworks.net/api/Orders/GetOrdersById"
    batch_size = 200
    total_batches = math.ceil(len(open_order_ids) / batch_size)
    orders_data = []

    for i in range(total_batches):
        batch_ids = open_order_ids[i * batch_size:(i + 1) * batch_size]
        payload = {"pkOrderIds": batch_ids}
        try:
            response = requests.post(url_order_details, json=payload, headers=headers)
            response.raise_for_status()
            orders = response.json()
            for order in orders:
                orders_data.append({
                    "_airbyte_raw_id": None,
                    "_airbyte_extracted_at": None,
                    "_airbyte_meta": None,
                    "_airbyte_generation_id": None,
                    "Items": json.dumps(order.get("Items")),
                    "Notes": json.dumps(order.get("Notes")),
                    "TaxId": order.get("TaxId") or '',
                    "OrderId": order.get("OrderId") or '',
                    "Processed": 0,
                    "FolderName": order.get("FolderName") or '',
                    "NumOrderId": order.get("NumOrderId"),
                    "TotalsInfo": json.dumps(order.get("TotalsInfo")),
                    "GeneralInfo": json.dumps(order.get("GeneralInfo")),
                    "CustomerInfo": json.dumps(order.get("CustomerInfo")),
                    "PaidDateTime": order.get("PaidDateTime"),
                    "ShippingInfo": json.dumps(order.get("ShippingInfo")),
                    "ProcessedDateTime": order.get("ProcessedDateTime"),
                    "ExtendedProperties": json.dumps(order.get("ExtendedProperties")),
                    "FulfilmentLocationId": order.get("FulfilmentLocationId") or ''
                })
            logging.info("Processed batch %d/%d", i + 1, total_batches)
            time.sleep(0.5)
        except Exception as e:
            logging.error("Error fetching order details batch %d: %s", i + 1, e)

    df_orders = pd.DataFrame(orders_data)
    logging.info("Total orders fetched: %d", len(df_orders))
    return df_orders

# ------------------------------
# Insert Orders into SQL
# ------------------------------
def insert_orders_to_sql(df, conn):
    if df.empty:
        logging.info("No orders to insert.")
        return

    cursor = conn.cursor()
    sql = """
    INSERT INTO [linnworks].[staging].[_airbyte_raw_processed_order_details] (
        _airbyte_raw_id, _airbyte_extracted_at, _airbyte_meta, _airbyte_generation_id,
        Items, Notes, TaxId, OrderId, Processed, FolderName, NumOrderId,
        TotalsInfo, GeneralInfo, CustomerInfo, PaidDateTime, ShippingInfo,
        ProcessedDateTime, ExtendedProperties, FulfilmentLocationId
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        for _, row in df.iterrows():
            cursor.execute(sql, (
                row["_airbyte_raw_id"],
                row["_airbyte_extracted_at"],
                row["_airbyte_meta"],
                row["_airbyte_generation_id"],
                row["Items"],
                row["Notes"],
                row["TaxId"],
                row["OrderId"],
                row["Processed"],
                row["FolderName"],
                row["NumOrderId"],
                row["TotalsInfo"],
                row["GeneralInfo"],
                row["CustomerInfo"],
                row["PaidDateTime"],
                row["ShippingInfo"],
                row["ProcessedDateTime"],
                row["ExtendedProperties"],
                row["FulfilmentLocationId"]
            ))
        conn.commit()
        logging.info("Inserted %d orders into SQL.", len(df))
    except Exception as e:
        logging.error("Error inserting orders into SQL: %s", e)
        conn.rollback()
    finally:
        cursor.close()

# ------------------------------
# Cloud Run Entrypoint
# ------------------------------
def main_openOrders(request: Request):
    try:
        logging.info("Starting OpenOrders processing...")

        access_token = get_linnworks_access_token()
        if not access_token:
            return ("Failed to get Linnworks access token.", 500)

        conn = pymssql.connect(server=SQL_SERVER, user=SQL_USER, password=SQL_PASSWORD, database=SQL_DATABASE)
        cursor = conn.cursor()
        completed, last_run_dt = airbyte_completed_today(cursor)

        if not completed:
            msg = f"Airbyte last run was {last_run_dt}, not today." if last_run_dt else "No Airbyte runs detected yet."
            logging.info(msg)
            return (msg, 200)

        clear_unprocessed_orders(conn)
        df_orders = fetch_open_orders(cursor, access_token)
        insert_orders_to_sql(df_orders, conn)
        conn.close()

        return (f"OpenOrders refreshed successfully. Inserted {len(df_orders)} rows.", 200)

    except Exception as e:
        logging.error("Error in main_openOrders: %s", e)
        return (f"Error: {e}", 500)
