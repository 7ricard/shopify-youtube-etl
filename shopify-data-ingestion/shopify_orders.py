import json
import requests
import google.auth
import time
from datetime import datetime, timezone
from google.cloud import bigquery

# Load Shopify credentials
with open("config.json", "r") as f:
    config = json.load(f)

SHOPIFY_STORE = config["shopify_store"]
API_KEY = config["api_key"]
PASSWORD = config["password"]
SHOPIFY_API_VERSION = "2024-01"
SHOPIFY_API_URL = f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/{SHOPIFY_API_VERSION}/orders.json"

# BigQuery Setup
client = bigquery.Client()
DATASET_ID = "Shopify"
TABLE_ID = "shopify_orders"
CONTROL_TABLE = "pipeline_metadata.sync_control"

class ShopifyETLPipeline:
    def __init__(self):
        self.control_table = CONTROL_TABLE

    def get_last_sync_timestamp(self):
        """Retrieve the last successful sync timestamp from BigQuery."""
        query = f"""
        SELECT last_sync_timestamp 
        FROM `{self.control_table}`
        WHERE table_name = 'shopify_orders'
        ORDER BY last_sync_timestamp DESC
        LIMIT 1
        """
        query_job = client.query(query)
        results = list(query_job.result())
        return results[0]["last_sync_timestamp"] if results else None

    def update_sync_timestamp(self, records_processed):
        """Update the last sync timestamp in BigQuery."""
        query = f"""
        MERGE INTO `{self.control_table}` T
        USING (SELECT 'shopify_orders' AS table_name, CURRENT_TIMESTAMP() AS last_sync_timestamp, {records_processed} AS records_processed) S
        ON T.table_name = S.table_name
        WHEN MATCHED THEN UPDATE SET T.last_sync_timestamp = S.last_sync_timestamp, T.records_processed = S.records_processed
        WHEN NOT MATCHED THEN INSERT (table_name, last_sync_timestamp, records_processed) VALUES (S.table_name, S.last_sync_timestamp, S.records_processed)
        """
        client.query(query).result()
        print(f"Updated last sync timestamp: {datetime.now(timezone.utc)}")

    def fetch_shopify_data(self, start_date=None):
        """Fetch all Shopify orders, handling pagination."""
        params = {
            "status": "any",
            "limit": 250,
            "order": "asc"
        }
        if start_date:
            params["updated_at_min"] = start_date

        all_orders = []
        page_count = 1
        next_page_info = None

        while True:
            print(f"Fetching page {page_count}...")

            if next_page_info:
                # If paginating, remove conflicting params and use page_info
                params = {"limit": 250, "page_info": next_page_info}

            response = requests.get(SHOPIFY_API_URL, params=params, auth=(API_KEY, PASSWORD))

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                print(f"Rate limit hit. Retrying in {retry_after} seconds...")
                time.sleep(retry_after)
                continue

            if response.status_code != 200:
                print(f"API Error: {response.status_code}, {response.text}")
                break

            orders = response.json().get("orders", [])
            if not orders:
                print("No more orders found, exiting pagination.")
                break  # No more records, exit loop

            for order in orders:
                all_orders.append({
                    "order_id": str(order["id"]),
                    "created_at": order["created_at"],
                    "updated_at": order["updated_at"],
                    "processed_at": order.get("processed_at"),
                    "total_price": float(order["total_price"]),
                    "subtotal_price": float(order["subtotal_price"]),
                    "total_tax": float(order["total_tax"]),
                    "total_discounts": float(order["total_discounts"]),
                    "currency": order["currency"],
                    "financial_status": order.get("financial_status", ""),
                    "fulfillment_status": order.get("fulfillment_status", ""),
                    "customer_locale": order.get("customer_locale", ""),
                    "customer_email": order.get("contact_email", ""),
                    "customer_id": str(order["customer"]["id"]) if order.get("customer") else None,
                    "buyer_accepts_marketing": order.get("buyer_accepts_marketing", False),
                    "discount_codes": json.dumps(order.get("discount_codes", [])),
                    "payment_gateway_names": json.dumps(order.get("payment_gateway_names", [])),
                    "source_name": order.get("source_name"),
                    "referring_site": order.get("referring_site")
                })

            print(f"Fetched {len(orders)} orders on page {page_count}. Total collected: {len(all_orders)}")

            # Extract `page_info` from Shopify API response for pagination
            link_header = response.headers.get("Link")
            if link_header and "rel=\"next\"" in link_header:
                next_page_info = link_header.split("page_info=")[-1].split(">")[0]
                page_count += 1
            else:
                print("Pagination complete.")
                break  # No more pages, exit loop

        return all_orders

    def load_to_bigquery(self, orders):
        """Upsert orders into BigQuery."""
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        errors = client.insert_rows_json(table_ref, orders)

        if errors:
            print(f"BigQuery insertion errors: {errors}")
        else:
            print(f"Successfully inserted {len(orders)} rows into {TABLE_ID}")

    def execute(self, force_full_load=False):
        """Main execution function to handle historical & incremental loads."""
        last_sync = self.get_last_sync_timestamp() if not force_full_load else None

        if last_sync is None:
            print("Performing full historical load from 2024-01-01")
            orders = self.fetch_shopify_data(start_date="2024-01-01")
        else:
            print(f"Performing incremental load since {last_sync}")
            orders = self.fetch_shopify_data(start_date=last_sync)

        if orders:
            self.load_to_bigquery(orders)
            self.update_sync_timestamp(len(orders))
        else:
            print("No new orders to load.")

# Usage:
pipeline = ShopifyETLPipeline()

# For historical load (only run once):
pipeline.execute(force_full_load=True)

# For regular incremental runs (new data only):
pipeline.execute()