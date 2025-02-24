import json
import requests
import google.auth
import time
from datetime import datetime, timezone
from google.cloud import bigquery
from requests.exceptions import ChunkedEncodingError
from google.api_core.exceptions import RetryError

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
CONTROL_TABLE = "pipeline_metadata.sync_control"

# Table definitions
TABLE_SCHEMAS = {
    "orders": [
        bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("total_price", "FLOAT"),
        bigquery.SchemaField("financial_status", "STRING"),
        bigquery.SchemaField("fulfillment_status", "STRING"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("source_name", "STRING"),
        bigquery.SchemaField("customer_id", "STRING"),
    ],
    "line_items": [
        bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("product_id", "STRING"),
        bigquery.SchemaField("variant_id", "STRING"),
        bigquery.SchemaField("product_name", "STRING"),
        bigquery.SchemaField("price", "FLOAT"),
        bigquery.SchemaField("quantity", "INTEGER"),
        bigquery.SchemaField("vendor", "STRING"),
    ],
    "customers": [
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("first_name", "STRING"),
        bigquery.SchemaField("last_name", "STRING"),
        bigquery.SchemaField("phone", "STRING"),
        bigquery.SchemaField("verified_email", "BOOLEAN"),
    ],
    "shipping_addresses": [
        bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("first_name", "STRING"),
        bigquery.SchemaField("last_name", "STRING"),
        bigquery.SchemaField("address1", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("province", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("zip", "STRING"),
    ],
    "discount_codes": [
        bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("discount_code", "STRING"),
        bigquery.SchemaField("discount_value", "FLOAT"),
    ],
    "marketing_consent": [
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("email_consent", "STRING"),
        bigquery.SchemaField("sms_consent", "STRING"),
    ],
}

def chunk_records(records, chunk_size=10000):
    """
    Yield successive chunk_size-sized chunks from the records list.
    """
    for i in range(0, len(records), chunk_size):
        yield records[i : i + chunk_size]

class ShopifyETLPipeline:
    def __init__(self):
        self.control_table = CONTROL_TABLE

    def create_tables_if_missing(self):
        """Ensure all tables exist in BigQuery before inserting data."""
        for table_name, schema in TABLE_SCHEMAS.items():
            table_ref = client.dataset(DATASET_ID).table(table_name)
            try:
                client.get_table(table_ref)  # Check if table exists
            except Exception:
                table = bigquery.Table(table_ref, schema=schema)
                client.create_table(table)
                print(f"Created missing table: {table_name}")

    def get_last_sync_timestamp(self):
        """Retrieve the last successful sync timestamp from BigQuery."""
        query = f"""
        SELECT last_sync_timestamp 
        FROM `{self.control_table}`
        WHERE table_name = 'orders'
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
        USING (SELECT 'orders' AS table_name, CURRENT_TIMESTAMP() AS last_sync_timestamp, {records_processed} AS records_processed) S
        ON T.table_name = S.table_name
        WHEN MATCHED THEN UPDATE SET T.last_sync_timestamp = S.last_sync_timestamp, T.records_processed = S.records_processed
        WHEN NOT MATCHED THEN INSERT (table_name, last_sync_timestamp, records_processed) VALUES (S.table_name, S.last_sync_timestamp, S.records_processed)
        """
        client.query(query).result()
        print(f"Updated last sync timestamp: {datetime.now(timezone.utc)}")

    def fetch_shopify_data(self, start_date=None, test_mode=False, max_pages=5000, max_retries=3):
        """Fetch orders, extract nested fields, handle null marketing consent, and structure data."""
        params = {"status": "any", "limit": 250, "order": "asc"}
        if start_date:
            params["updated_at_min"] = start_date

        all_orders = []
        all_line_items = []
        all_customers = []
        all_shipping_addresses = []
        all_discount_codes = []
        all_marketing_consent = []

        page_count, total_fetched = 1, 0
        while True:
            if page_count > max_pages:
                print(f"Max page limit of {max_pages} reached, stopping.")
                break

            print(f"Fetching page {page_count}...")

            success = False
            for attempt in range(max_retries):
                try:
                    response = requests.get(SHOPIFY_API_URL, params=params, auth=(API_KEY, PASSWORD))
                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 60))
                        print(f"Rate limit hit. Retrying in {retry_after} seconds...")
                        time.sleep(retry_after)
                        continue
                    elif response.status_code != 200:
                        print(f"API Error: {response.status_code}, {response.text}")
                        success = False
                    else:
                        success = True
                    break
                except ChunkedEncodingError as e:
                    print(f"ChunkedEncodingError on attempt {attempt+1}: {e}")
                    time.sleep(3)
            if not success:
                print("Failed to fetch data after multiple attempts; exiting.")
                break

            orders = response.json().get("orders", [])
            if not orders:
                print("No more orders found, exiting pagination.")
                break

            for order in orders:
                # 1) Orders
                all_orders.append({
                    "order_id": str(order["id"]),
                    "created_at": order["created_at"],
                    "updated_at": order["updated_at"],
                    "total_price": float(order["total_price"]),
                    "financial_status": order.get("financial_status", ""),
                    "fulfillment_status": order.get("fulfillment_status", ""),
                    "currency": order["currency"],
                    "source_name": order.get("source_name", ""),
                    "customer_id": str(order["customer"]["id"]) if order.get("customer") else None
                })

                # 2) Line Items
                for item in order.get("line_items", []):
                    all_line_items.append({
                        "order_id": str(order["id"]),
                        "product_id": str(item.get("product_id")),
                        "variant_id": str(item.get("variant_id")),
                        "product_name": item.get("name"),
                        "price": float(item.get("price")),
                        "quantity": item.get("quantity"),
                        "vendor": item.get("vendor", "")
                    })

                # 3) Customers
                customer = order.get("customer")
                if customer and isinstance(customer, dict):
                    all_customers.append({
                        "customer_id": str(customer["id"]),
                        "email": customer.get("email", ""),
                        "created_at": customer.get("created_at", ""),
                        "first_name": customer.get("first_name", ""),
                        "last_name": customer.get("last_name", ""),
                        "phone": customer.get("phone", ""),
                        "verified_email": customer.get("verified_email", False)
                    })

                    # Short-circuit null marketing consent
                    email_consent = (customer.get("email_marketing_consent") or {}).get("state", "")
                    sms_consent = (customer.get("sms_marketing_consent") or {}).get("state", "")
                    if email_consent or sms_consent:
                        all_marketing_consent.append({
                            "customer_id": str(customer["id"]),
                            "email_consent": email_consent,
                            "sms_consent": sms_consent
                        })

                # 4) Shipping Address
                shipping = order.get("shipping_address", {})
                if shipping:
                    all_shipping_addresses.append({
                        "order_id": str(order["id"]),
                        "first_name": shipping.get("first_name", ""),
                        "last_name": shipping.get("last_name", ""),
                        "address1": shipping.get("address1", ""),
                        "city": shipping.get("city", ""),
                        "province": shipping.get("province", ""),
                        "country": shipping.get("country", ""),
                        "zip": shipping.get("zip", "")
                    })

                # 5) Discount Codes
                for discount in order.get("discount_codes", []):
                    all_discount_codes.append({
                        "order_id": str(order["id"]),
                        "discount_code": discount.get("code", ""),
                        "discount_value": float(discount.get("amount", 0))
                    })

            total_fetched += len(orders)
            print(f"Fetched {len(orders)} orders on page {page_count}. Total collected: {total_fetched}")

            if test_mode and total_fetched >= 500:
                print("Test batch limit reached, stopping early.")
                break

            page_count += 1
            next_page_info = response.headers.get("Link")
            if not next_page_info or "rel=\"next\"" not in next_page_info:
                print("Pagination complete.")
                break

        print("Exiting pagination loop... returning all data.")
        return (
            all_orders,
            all_line_items,
            all_customers,
            all_shipping_addresses,
            all_discount_codes,
            all_marketing_consent
        )

    def chunk_records(self, records, chunk_size=10000):
        """Yield successive chunk_size-sized chunks from the records list."""
        for i in range(0, len(records), chunk_size):
            yield records[i:i+chunk_size]

    def load_to_bigquery(self, table_name, records, chunk_size=10000):
        """Upsert records into BigQuery in smaller chunks to avoid large single calls."""
        if not records:
            print(f"No records to insert into {table_name}.")
            return

        table_ref = client.dataset(DATASET_ID).table(table_name)
        total_inserted = 0

        for chunk in self.chunk_records(records, chunk_size=chunk_size):
            print(f"Preparing to insert {len(chunk)} records into '{table_name}' chunk...")
            errors = client.insert_rows_json(table_ref, chunk)
            if errors:
                print(f"BigQuery insertion errors for table {table_name}: {errors}")
            else:
                total_inserted += len(chunk)
                print(f"Successfully inserted {len(chunk)} rows into {table_name}")

        print(f"Done inserting {total_inserted} total rows into {table_name}.")

    def execute(self, force_full_load=False, test_mode=False, max_pages=5000):
        """Main execution function to handle historical & incremental loads."""
        self.create_tables_if_missing()

        last_sync = self.get_last_sync_timestamp() if not force_full_load else None
        start_date = "2024-01-01" if last_sync is None else last_sync

        print(f"Performing {'test' if test_mode else 'full'} load from {start_date}")
        (
            orders,
            line_items,
            customers,
            shipping_addresses,
            discount_codes,
            marketing_consent
        ) = self.fetch_shopify_data(start_date=start_date, test_mode=test_mode, max_pages=max_pages)

        # Load each table in 10k-chunks
        self.load_to_bigquery("orders", orders, chunk_size=10000)
        self.load_to_bigquery("line_items", line_items, chunk_size=10000)
        self.load_to_bigquery("customers", customers, chunk_size=10000)
        self.load_to_bigquery("shipping_addresses", shipping_addresses, chunk_size=10000)
        self.load_to_bigquery("discount_codes", discount_codes, chunk_size=10000)
        self.load_to_bigquery("marketing_consent", marketing_consent, chunk_size=10000)

        print("Insertion to BigQuery done.")
        self.update_sync_timestamp(len(orders))
        print("All done. Exiting now.")

if __name__ == "__main__":
    pipeline = ShopifyETLPipeline()
    # test_mode=False, max_pages=100 to test partial run
    pipeline.execute(force_full_load=True, test_mode=False, max_pages=100)