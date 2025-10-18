import os
import requests
import pymssql
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Request
import time

logging.basicConfig(level=logging.INFO)

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DATABASE = os.getenv("SQL_DATABASE")

LINNWORKS_APP_ID = os.getenv("LINNWORKS_APP_ID")
LINNWORKS_APP_SECRET = os.getenv("LINNWORKS_APP_SECRET")
LINNWORKS_TOKEN = os.getenv("LINNWORKS_TOKEN")


def safe_date(value):
    """Convert ISO8601 string to datetime or None."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def safe_str(value):
    """Convert value to lowercase string or None."""
    if value is None:
        return None
    return str(value).strip().lower()


class LinnworksAPI:
    """Handles Linnworks API authentication and data retrieval."""

    def __init__(self, app_id, app_secret, token):
        self.app_id = app_id
        self.app_secret = app_secret
        self.token = token
        self.access_token = None

    def get_access_token(self):
        """Fetches Linnworks access token."""
        if self.access_token:
            return self.access_token
        url = "https://api.linnworks.net/api/Auth/AuthorizeByApplication"
        payload = {"ApplicationId": self.app_id, "ApplicationSecret": self.app_secret, "Token": self.token}
        try:
            r = requests.post(url, json=payload)
            r.raise_for_status()
            data = r.json()
            self.access_token = data.get("Token") or data.get("AccessToken")
            if not self.access_token:
                logging.error("Access token not found in response: %s", data)
                return None
            logging.info("Fetched Linnworks access token successfully.")
            return self.access_token
        except Exception as e:
            logging.error("Error fetching Linnworks access token: %s", e)
            return None

    def get_purchase_orders_summary(self):
        """Fetches all purchase order summaries."""
        token = self.get_access_token()
        if not token:
            return []

        all_data, page_number, total_pages = [], 1, 1
        headers = {"Authorization": token, "Accept": "application/json", "Content-Type": "application/json"}

        while page_number <= total_pages:
            payload = {"searchParameters": {"DateFrom": "2020-01-01T00:00:00", "DateTo": datetime.utcnow().isoformat()},
                       "entriesPerPage": 100, "pageNumber": page_number}
            r = requests.post("https://eu-ext.linnworks.net/api/PurchaseOrder/Search_PurchaseOrders2",
                              json=payload, headers=headers)
            r.raise_for_status()
            json_data = r.json()
            all_data.extend(json_data.get("Result", []))
            total_pages = json_data.get("TotalPages", 1)
            page_number += 1

        logging.info("Fetched %d purchase order summaries.", len(all_data))
        return all_data

    def get_purchase_order_details(self, pkPurchaseID):
        """Fetches full purchase order details."""
        token = self.get_access_token()
        if not token:
            return None
        url = "https://eu-ext.linnworks.net/api/PurchaseOrder/Get_PurchaseOrder"
        headers = {"Authorization": token, "Accept": "application/json", "Content-Type": "application/json"}
        payload = {"pkPurchaseId": pkPurchaseID}
        r = requests.post(url, json=payload, headers=headers)
        if r.status_code != 200:
            logging.warning("Failed to fetch PO %s", pkPurchaseID)
            return None
        return r.json()

    def flatten_purchase_order(self, po):
        """Flattens a single purchase order into rows for DB insertion."""
        header = po.get("PurchaseOrderHeader", {})
        items = po.get("PurchaseOrderItem", [])
        delivered = po.get("DeliveredRecords", [])
        rows = []

        for item in items:
            matched_deliveries = [d for d in delivered if d.get("fkPurchaseItemId") == item.get("pkPurchaseItemId")]
            if matched_deliveries:
                for dr in matched_deliveries:
                    rows.append(self.build_row(item, header, dr))
            else:
                rows.append(self.build_row(item, header, {}))
        return rows

    @staticmethod
    def build_row(item, header, delivery):
        """Constructs a flattened row for DB."""
        return {
            "PurchaseOrderItem_pkPurchaseItemId": item.get("pkPurchaseItemId"),
            "PurchaseOrderItem_fkStockItemId": item.get("fkStockItemId"),
            "PurchaseOrderItem_StockItemIntId": item.get("StockItemIntId"),
            "PurchaseOrderItem_Quantity": item.get("Quantity"),
            "PurchaseOrderItem_Cost": item.get("Cost"),
            "PurchaseOrderItem_Delivered": item.get("Delivered"),
            "PurchaseOrderItem_TaxRate": item.get("TaxRate"),
            "PurchaseOrderItem_Tax": item.get("Tax"),
            "PurchaseOrderItem_PackQuantity": item.get("PackQuantity"),
            "PurchaseOrderItem_PackSize": item.get("PackSize"),
            "PurchaseOrderItem_SKU": item.get("SKU"),
            "PurchaseOrderItem_ItemTitle": item.get("ItemTitle"),
            "PurchaseOrderItem_InventoryTrackingType": item.get("InventoryTrackingType"),
            "PurchaseOrderItem_IsDeleted": item.get("IsDeleted"),
            "PurchaseOrderItem_SortOrder": item.get("SortOrder"),
            "PurchaseOrderItem_DimHeight": item.get("DimHeight"),
            "PurchaseOrderItem_DimWidth": item.get("DimWidth"),
            "PurchaseOrderItem_BarcodeNumber": item.get("BarcodeNumber"),
            "PurchaseOrderItem_DimDepth": item.get("DimDepth"),
            "PurchaseOrderItem_BoundToOpenOrdersItems": item.get("BoundToOpenOrdersItems"),
            "PurchaseOrderItem_QuantityBoundToOpenOrdersItems": item.get("QuantityBoundToOpenOrdersItems"),
            "PurchaseOrderItem_SupplierCode": item.get("SupplierCode"),
            "PurchaseOrderItem_SupplierBarcode": item.get("SupplierBarcode"),
            "PurchaseOrderItem_SkuGroupIds": str(item.get("SkuGroupIds") or []),
            "PurchaseOrderHeader_pkPurchaseID": header.get("pkPurchaseID"),
            "PurchaseOrderHeader_ExternalInvoiceNumber": header.get("ExternalInvoiceNumber"),
            "PurchaseOrderHeader_Status": header.get("Status"),
            "PurchaseOrderHeader_DateOfPurchase": header.get("DateOfPurchase"),
            "PurchaseOrderHeader_DateOfDelivery": header.get("DateOfDelivery"),
            "PurchaseOrderHeader_TotalCost": header.get("TotalCost"),
            "DeliveredRecords_pkDeliveryRecordId": delivery.get("pkDeliveryRecordId"),
            "DeliveredRecords_fkPurchaseItemId": delivery.get("fkPurchaseItemId"),
            "DeliveredRecords_fkStockLocationId": delivery.get("fkStockLocationId"),
            "DeliveredRecords_UnitCost": delivery.get("UnitCost"),
            "DeliveredRecords_DeliveredQuantity": delivery.get("DeliveredQuantity"),
            "DeliveredRecords_CreatedDateTime": delivery.get("CreatedDateTime"),
            "DeliveredRecords_fkBatchInventoryId": delivery.get("fkBatchInventoryId"),
            "DeliveredRecords_ModifiedDateTime": delivery.get("ModifiedDateTime")
        }

    def fetch_all_purchase_orders_parallel(self, batch_size=10, sleep_between_batches=1.5):
        """Fetches and flattens all purchase orders in parallel batches."""
        summary = self.get_purchase_orders_summary()
        all_rows = []
        total = len(summary)
        logging.info("Total purchase orders to fetch: %d", total)

        for start in range(0, total, batch_size):
            batch = summary[start:start + batch_size]

            def fetch_and_flatten(order):
                details = self.get_purchase_order_details(order.get("pkPurchaseID"))
                return self.flatten_purchase_order(details) if details else []

            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                futures = [executor.submit(fetch_and_flatten, order) for order in batch]
                for future in as_completed(futures):
                    all_rows.extend(future.result())

            logging.info("Processed %d / %d purchase orders...", min(start + batch_size, total), total)
            time.sleep(sleep_between_batches)

        logging.info("Finished fetching all purchase orders. Total flattened rows: %d", len(all_rows))
        return all_rows


class DatabaseLoader:
    """Handles database operations for stock and full purchase orders."""

    def __init__(self, server, user, password, database):
        self.server = server
        self.user = user
        self.password = password
        self.database = database

    def push_stock_orders(self, records):
        """Insert stock-level purchase orders into lw.PurchaseOrders table."""
        if not records:
            logging.info("No stock orders to insert.")
            return
        conn = pymssql.connect(self.server, self.user, self.password, self.database)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT pkPurchaseID FROM lw.PurchaseOrders")
            existing_ids = {safe_str(row[0]) for row in cursor.fetchall() if row[0]}
            new_records = [r for r in records if r.get("pkPurchaseID") and safe_str(r["pkPurchaseID"]) not in existing_ids]
            logging.info("New stock orders to insert: %d", len(new_records))
            if not new_records:
                return

            sql = """INSERT INTO lw.PurchaseOrders (
                pkPurchaseID, fkSupplierId, fkLocationId, ExternalInvoiceNumber, Status, Currency, SupplierReferenceNumber,
                Locked, LineCount, DeliveredLinesCount, UnitAmountTaxIncludedType, DateOfPurchase, DateOfDelivery, QuotedDeliveryDate,
                PostagePaid, TotalCost, taxPaid, ShippingTaxRate, ConversionRate, ConvertedShippingCost,
                ConvertedShippingTax, ConvertedOtherCost, ConvertedOtherTax, ConvertedGrandTotal
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """

            for row in new_records:
                cursor.execute(sql, (
                    row.get("pkPurchaseID"),
                    row.get("fkSupplierId"),
                    row.get("fkLocationId"),
                    row.get("ExternalInvoiceNumber"),
                    row.get("Status"),
                    row.get("Currency"),
                    row.get("SupplierReferenceNumber"),
                    1 if row.get("Locked") else 0,
                    row.get("LineCount") or 0,
                    row.get("DeliveredLinesCount") or 0,
                    str(row.get("UnitAmountTaxIncludedType") or ""),
                    safe_date(row.get("DateOfPurchase")),
                    safe_date(row.get("DateOfDelivery")),
                    safe_date(row.get("QuotedDeliveryDate")),
                    row.get("PostagePaid") or 0,
                    row.get("TotalCost") or 0,
                    row.get("taxPaid") or 0,
                    row.get("ShippingTaxRate") or 0,
                    row.get("ConversionRate") or 0,
                    row.get("ConvertedShippingCost") or 0,
                    row.get("ConvertedShippingTax") or 0,
                    row.get("ConvertedOtherCost") or 0,
                    row.get("ConvertedOtherTax") or 0,
                    row.get("ConvertedGrandTotal") or 0
                ))
            conn.commit()
            logging.info("Inserted %d new stock orders.", len(new_records))
        except Exception as e:
            logging.error("Error inserting stock orders: %s", e)
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    def load_full_purchase_orders(self, all_rows):
        """Truncate and insert full purchase orders into staging.FullPurchaseOrders."""
        if not all_rows:
            logging.info("No full purchase orders to insert.")
            return
        conn = pymssql.connect(self.server, self.user, self.password, self.database)
        cursor = conn.cursor()
        try:
            cursor.execute("TRUNCATE TABLE [staging].[FullPurchaseOrders]")
            columns = list(all_rows[0].keys())
            placeholders = ','.join(['%s'] * len(columns))
            sql = f"INSERT INTO [staging].[FullPurchaseOrders] ({','.join(columns)}) VALUES ({placeholders})"
            for row in all_rows:
                cursor.execute(sql, [row[col] for col in columns])
            conn.commit()
            logging.info("Inserted %d rows into [staging].[FullPurchaseOrders]", len(all_rows))
        except Exception as e:
            logging.error("Error inserting full purchase orders: %s", e)
            conn.rollback()
        finally:
            cursor.close()
            conn.close()


def full_purchase_orders_loader(request: Request):
    """Cloud Run HTTP entrypoint for loading Linnworks stock and full purchase orders."""
    try:
        api = LinnworksAPI(LINNWORKS_APP_ID, LINNWORKS_APP_SECRET, LINNWORKS_TOKEN)
        db_loader = DatabaseLoader(SQL_SERVER, SQL_USER, SQL_PASSWORD, SQL_DATABASE)

        # Step 1: Process stock-level POs
        stock_orders = api.get_purchase_orders_summary()
        db_loader.push_stock_orders(stock_orders)

        # Step 2: Process full POs with details
        full_po_rows = api.fetch_all_purchase_orders_parallel()
        db_loader.load_full_purchase_orders(full_po_rows)

        return f"Successfully loaded {len(full_po_rows)} full purchase orders.", 200
    except Exception as e:
        logging.error("Error in full_purchase_orders_loader: %s", e)
        return f"Error: {e}", 500
