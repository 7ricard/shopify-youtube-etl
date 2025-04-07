import json
import requests
import google.auth
import time
import uuid
from datetime import datetime, timezone
from google.cloud import bigquery, storage
from requests.exceptions import ChunkedEncodingError
from google.api_core.exceptions import RetryError
import traceback
import re

###########################################################################
# User config
###########################################################################
GCS_BUCKET_NAME = "my-shopify-bucket"

SHOPIFY_STORE = "[Hidden For Confidentiality]"
API_KEY = "[Api Key Hidden For Confidentiality]"
PASSWORD = "[Password Hidden For Confidentiality]"
SHOPIFY_API_VERSION = "2024-01"
SHOPIFY_API_URL = f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/{SHOPIFY_API_VERSION}/orders.json"

bigquery_client = bigquery.Client()
storage_client = storage.Client()

DATASET_ID = "Shopify"
CONTROL_TABLE = "pipeline_metadata.sync_control"

################################################################
# Final + Staging Table Schemas & Unique Keys
################################################################
FINAL_TABLE_SCHEMAS = {
    "orders": [
        bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("processed_at", "TIMESTAMP"), # added field to include original order processing f/ woocommerce
        bigquery.SchemaField("subtotal_price", "FLOAT"), # added field for analysis 
        bigquery.SchemaField("total_tax", "FLOAT"), # added to calculate tax rate
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

UNIQUE_KEYS = {
    "orders": ["order_id"],
    "line_items": ["order_id", "product_id", "variant_id"],
    "customers": ["customer_id"],
    "shipping_addresses": ["order_id", "first_name", "last_name"],
    "discount_codes": ["order_id", "discount_code"],
    "marketing_consent": ["customer_id"]
}

###########################################################################
# Helper Functions
###########################################################################
def chunk_records(records, chunk_size=10000):
    """Yield successive chunk_size-sized chunks from the records list."""
    for i in range(0, len(records), chunk_size):
        yield records[i:i+chunk_size]

def write_chunk_to_gcs(table_name, chunk, chunk_index):
    """
    Convert 'chunk' (list of dicts) to NDJSON lines,
    upload to GCS bucket, return the GCS URI.
    """
    lines = []
    for row in chunk:
        lines.append(json.dumps(row))
    file_contents = "\n".join(lines)

    # Create a unique object name
    object_name = f"{table_name}/{uuid.uuid4()}_chunk{chunk_index}.ndjson"
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(object_name)
    blob.upload_from_string(file_contents, content_type="application/json")

    gcs_uri = f"gs://{GCS_BUCKET_NAME}/{object_name}"
    print(f"Uploaded chunk {chunk_index} for {table_name} to {gcs_uri}")
    return gcs_uri

def load_gcs_file_to_staging(table_name, gcs_uri):
    """
    Use a BigQuery Load Job to load NDJSON from GCS into <table_name>_staging.
    """
    staging_table_id = f"{DATASET_ID}.{table_name}_staging"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=FINAL_TABLE_SCHEMAS[table_name],
        write_disposition="WRITE_APPEND",  # accumulate chunks
        ignore_unknown_values=True
    )

    load_job = bigquery_client.load_table_from_uri(
        gcs_uri,
        staging_table_id,
        job_config=job_config,
    )
    load_job.result()  # block until done
    print(f"Loaded {gcs_uri} into {staging_table_id} via Load Job.")

###########################################################################
# The Main Pipeline Class
###########################################################################
class ShopifyETLPipeline:
    def __init__(self):
        self.control_table = CONTROL_TABLE

    def create_final_and_staging_tables_if_missing(self):
        """Create final tables and their staging tables if missing (unchanged)."""
        for table_name, schema in FINAL_TABLE_SCHEMAS.items():
            final_ref = bigquery_client.dataset(DATASET_ID).table(table_name)
            try:
                bigquery_client.get_table(final_ref)
            except Exception:
                final_table = bigquery.Table(final_ref, schema=schema)
                bigquery_client.create_table(final_table)
                print(f"Created missing final table: {table_name}")

            staging_ref = bigquery_client.dataset(DATASET_ID).table(f"{table_name}_staging")
            try:
                bigquery_client.get_table(staging_ref)
            except Exception:
                staging_table = bigquery.Table(staging_ref, schema=schema)
                bigquery_client.create_table(staging_table)
                print(f"Created missing staging table: {table_name}_staging")

    def get_last_sync_timestamp(self):
        """Get the last successful sync timestamp with enhanced error handling."""
        try:
            query = f"""
            SELECT last_sync_timestamp 
            FROM `{self.control_table}`
            WHERE table_name = 'orders' AND status = 'success'
            ORDER BY last_sync_timestamp DESC
            LIMIT 1
            """
            query_job = bigquery_client.query(query)
            results = list(query_job.result())
            
            if not results:
                print("No previous successful sync found. Performing initial load.")
                return None
                
            last_sync = results[0]["last_sync_timestamp"]
            print(f"Last successful sync: {last_sync}")
            
            # Add a small buffer (subtract 1 hour) to ensure no records are missed
            # due to timezone issues or processing delays
            buffer_time = datetime.timedelta(hours=1)
            adjusted_sync = last_sync - buffer_time
            
            # Format for Shopify API
            formatted_sync = adjusted_sync.strftime("%Y-%m-%dT%H:%M:%S%z")
            print(f"Using adjusted sync time (with 1hr buffer): {formatted_sync}")
            
            return formatted_sync
        except Exception as e:
            print(f"Error retrieving last sync timestamp: {e}")
            print(traceback.format_exc())
            print("Defaulting to a safe fallback date")
            return "2024-01-01T00:00:00+00:00"  # Safe fallback

    def update_sync_timestamp(self, records_processed, status="success"):
        """Update sync control table with enhanced metadata."""
        current_time = datetime.now(timezone.utc)
        run_id = str(uuid.uuid4())
        
        query = f"""
        INSERT INTO `{self.control_table}` 
        (table_name, last_sync_timestamp, records_processed, status, run_id, notes)
        VALUES 
        ('orders', @timestamp, {records_processed}, @status, @run_id, @notes)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", current_time),
                bigquery.ScalarQueryParameter("status", "STRING", status),
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
                bigquery.ScalarQueryParameter("notes", "STRING", f"Processed {records_processed} records")
            ]
        )
        
        try:
            bigquery_client.query(query, job_config=job_config).result()
            print(f"Updated sync metadata: timestamp={current_time}, status={status}, run_id={run_id}")
            return True
        except Exception as e:
            print(f"Failed to update sync metadata: {e}")
            print(traceback.format_exc())
            return False

    def create_control_table_if_missing(self):
        """Ensure control table exists with proper schema."""
        control_dataset, control_table = self.control_table.split(".")
        
        # Check if dataset exists, create if not
        try:
            bigquery_client.get_dataset(control_dataset)
        except Exception:
            dataset = bigquery.Dataset(f"{bigquery_client.project}.{control_dataset}")
            dataset.location = "US"  # Set your preferred location
            bigquery_client.create_dataset(dataset)
            print(f"Created missing dataset: {control_dataset}")
        
        # Check if control table exists, create if not
        full_table_id = f"{bigquery_client.project}.{self.control_table}"
        try:
            bigquery_client.get_table(full_table_id)
        except Exception:
            schema = [
                bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("last_sync_timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("records_processed", "INTEGER"),
                bigquery.SchemaField("status", "STRING"),  # success, error, partial
                bigquery.SchemaField("run_id", "STRING"),  # unique identifier for each run
                bigquery.SchemaField("notes", "STRING"),   # any additional context
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", 
                                    default_value_expression="CURRENT_TIMESTAMP()")
            ]
            table = bigquery.Table(full_table_id, schema=schema)
            bigquery_client.create_table(table)
            print(f"Created missing control table: {self.control_table}")



    def fetch_shopify_data(self, start_date=None, test_mode=False, max_pages=5000, max_retries=5):
        """Fetch orders, parse nested fields, handle marketing consent, and return all 6 lists.
        This version uses a 30-second timeout and exponential backoff with detailed error logging."""
        params = {"status": "any", "limit": 250, "order": "asc"}
        if start_date:
            params["updated_at_min"] = start_date

        all_orders = []
        all_line_items = []
        all_customers = []
        all_shipping_addresses = []
        all_discount_codes = []
        all_marketing_consent = []

        # Set to keep track of order IDs we've already processed
        processed_order_ids = set()

        page_count, total_fetched = 1, 0
        next_url = SHOPIFY_API_URL
        
        while next_url and page_count <= max_pages:
            print(f"Fetching page {page_count}...")

            success = False
            for attempt in range(max_retries):
                try:
                    if page_count == 1:
                        response = requests.get(
                            SHOPIFY_API_URL,
                            params=params,
                            auth=(API_KEY, PASSWORD),
                            timeout=30
                        )
                    else:
                        # For subsequent pages, we use the next_url directly without adding params
                        response = requests.get(
                            next_url,
                            auth=(API_KEY, PASSWORD),
                            timeout=30
                        )
                    
                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 60))
                        print(f"Rate limit hit. Retrying in {retry_after} seconds (attempt {attempt+1}).")
                        time.sleep(retry_after)
                        continue
                    elif response.status_code != 200:
                        print(f"API Error on attempt {attempt+1}: {response.status_code}, {response.text}")
                        success = False
                    else:
                        success = True
                    break
                except Exception as e:
                    delay = 3 * (2 ** attempt)
                    print(f"Exception on attempt {attempt+1}: {e}. Retrying in {delay} seconds.")
                    print(traceback.format_exc())
                    time.sleep(delay)
            
            if not success:
                print("Failed to fetch data after multiple attempts; exiting pagination.")
                break

            orders = response.json().get("orders", [])
            if not orders:
                print("No more orders found, exiting pagination.")
                break

            new_orders_count = 0
            for order in orders:
                order_id = str(order["id"])
                
                # Skip if we've already processed this order
                if order_id in processed_order_ids:
                    print(f"Skipping duplicate order_id: {order_id}")
                    continue
                
                processed_order_ids.add(order_id)
                new_orders_count += 1
                
                # 1) Orders
                all_orders.append({
                    "order_id": order_id,
                    "created_at": order["created_at"],
                    "updated_at": order["updated_at"],
                    "processed_at": order["processed_at"],
                    "subtotal_price": float(order["subtotal_price"]),
                    "total_price": float(order["total_price"]),
                    "total_tax": float(order["total_tax"]),
                    "financial_status": order.get("financial_status", ""),
                    "fulfillment_status": order.get("fulfillment_status", ""),
                    "currency": order["currency"],
                    "source_name": order.get("source_name", ""),
                    "customer_id": str(order["customer"]["id"]) if order.get("customer") else None
                })

                # Process other data same as before...
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
                if order.get("customer"):
                    customer = order["customer"]
                    all_customers.append({
                        "customer_id": str(customer.get("id")),
                        "email": customer.get("email", ""),
                        "created_at": customer.get("created_at"),
                        "first_name": customer.get("first_name", ""),
                        "last_name": customer.get("last_name", ""),
                        "phone": customer.get("phone", ""),
                        "verified_email": customer.get("verified_email", False)
                    })

                # 4) Shipping Addresses
                if order.get("shipping_address"):
                    address = order["shipping_address"]
                    all_shipping_addresses.append({
                        "order_id": order_id,
                        "first_name": address.get("first_name", ""),
                        "last_name": address.get("last_name", ""),
                        "address1": address.get("address1", ""),
                        "city": address.get("city", ""),
                        "province": address.get("province", ""),
                        "country": address.get("country", ""),
                        "zip": address.get("zip", "")
                    })

                # 5) Discount Codes
                if order.get("discount_codes"):
                    for discount in order["discount_codes"]:
                        all_discount_codes.append({
                            "order_id": order_id,
                            "discount_code": discount.get("code", ""),
                            "discount_value": float(discount.get("amount", 0.0))
                        })

                # 6) Marketing Consent
                # Using the customer's marketing flag, if available.
                if order.get("customer"):
                    customer = order["customer"]
                    email_consent = "yes" if customer.get("accepts_marketing", False) else "no"
                    # Shopify doesn't provide SMS consent directly; adjust if you have an alternate source.
                    sms_consent = ""
                    all_marketing_consent.append({
                        "customer_id": str(customer.get("id")),
                        "email_consent": email_consent,
                        "sms_consent": sms_consent
                    })
                
            total_fetched += new_orders_count
            print(f"Fetched {len(orders)} orders (added {new_orders_count} new) on page {page_count}. Total collected: {total_fetched}")

            if test_mode and total_fetched >= 500:
                print("Test batch limit reached, stopping early.")
                break

            # Extract the next page URL from the Link header
            next_url = None
            link_header = response.headers.get("Link", "")
            if link_header and "rel=\"next\"" in link_header:
                # Parse the 'next' URL from the Link header
                for link in link_header.split(","):
                    if "rel=\"next\"" in link:
                        url_match = re.search("<(.+?)>", link)
                        if url_match:
                            next_url = url_match.group(1)
                            break
            
            if not next_url:
                print("No next page URL found. Pagination complete.")
                break
                
            page_count += 1

        print(f"Exiting pagination loop... returning {len(all_orders)} orders.")
        return (
            all_orders,
            all_line_items,
            all_customers,
            all_shipping_addresses,
            all_discount_codes,
            all_marketing_consent
        )

    def inspect_duplicates(records, key_field):
        """Inspect records to check for duplicates based on key_field."""
        if not records:
            print("No records to inspect.")
            return
        
        key_count = {}
        for record in records:
            key = record.get(key_field)
            if key is not None:
                key_count[key] = key_count.get(key, 0) + 1
        
        duplicates = {k: v for k, v in key_count.items() if v > 1}
        
        if duplicates:
            print(f"Found {len(duplicates)} duplicate keys out of {len(key_count)} unique keys")
            print(f"Examples of duplicated keys: {list(duplicates.items())[:5]}")
        else:
            print(f"No duplicates found. {len(key_count)} unique keys in {len(records)} records.")

    def load_table_via_gcs(self, table_name, records, chunk_size=10000):
        """
        For each chunk of 'records':
        - write NDJSON chunk to GCS
        - run a BigQuery Load Job to <table_name>_staging with 'WRITE_APPEND'
        """
        if not records:
            print(f"No records for {table_name}.")
            return 0

        # Debug information to understand the data
        print(f"Initial {table_name} record count: {len(records)}")
        
        # Only deduplicate if there are actual duplicates
        unique_cols = UNIQUE_KEYS[table_name]
        
        # Count distinct values of the unique key
        if len(unique_cols) == 1:
            unique_key = unique_cols[0]
            unique_values = set(record.get(unique_key) for record in records if record.get(unique_key) is not None)
            unique_count = len(unique_values)
            print(f"Distinct values for {unique_key}: {unique_count} out of {len(records)} records")
            
            # Only deduplicate if there are actual duplicates
            if unique_count < len(records):
                seen = set()
                deduplicated_records = []
                for record in records:
                    key = record.get(unique_key)
                    if key and key not in seen:
                        seen.add(key)
                        deduplicated_records.append(record)
                records = deduplicated_records
                print(f"Deduplicated {table_name} records: {len(records)} unique records")
        elif len(unique_cols) > 1:
            # For composite keys
            seen = set()
            valid_records = []
            for record in records:
                # Only consider records where all unique key components exist
                if all(record.get(col) is not None for col in unique_cols):
                    valid_records.append(record)
                    key = tuple(str(record.get(col)) for col in unique_cols)
                    seen.add(key)
            
            print(f"Distinct composite keys: {len(seen)} out of {len(records)} records")
            if len(seen) < len(valid_records):
                seen.clear()
                deduplicated_records = []
                for record in records:
                    if all(record.get(col) is not None for col in unique_cols):
                        key = tuple(str(record.get(col)) for col in unique_cols)
                        if key not in seen:
                            seen.add(key)
                            deduplicated_records.append(record)
                    else:
                        # Keep records that don't have complete unique keys
                        deduplicated_records.append(record)
                records = deduplicated_records
                print(f"Deduplicated {table_name} records: {len(records)} unique records")
        
        total_loaded = 0
        chunk_index = 0
        for chunk in chunk_records(records, chunk_size):
            # 1) Write chunk to GCS
            gcs_uri = write_chunk_to_gcs(table_name, chunk, chunk_index)
            # 2) Load job from GCS => staging
            load_gcs_file_to_staging(table_name, gcs_uri)

            total_loaded += len(chunk)
            chunk_index += 1

        print(f"Done loading {total_loaded} rows into {table_name}_staging via GCS Load Jobs.\n")
        return total_loaded

    def merge_staging_to_final(self, table_name):
        """
        Single MERGE from <table_name>_staging => <table_name>, then TRUNCATE staging
        (No streaming, so no streaming buffer conflict).
        """
        unique_cols = UNIQUE_KEYS[table_name]
        on_clause = " AND ".join([f"T.{col}=S.{col}" for col in unique_cols])

        cols = [field.name for field in FINAL_TABLE_SCHEMAS[table_name]]
        update_set = ", ".join([f"T.{col}=S.{col}" for col in cols if col not in unique_cols])
        insert_cols = ", ".join(cols)
        insert_vals = ", ".join([f"S.{col}" for col in cols])

        # Add a subquery with DISTINCT to eliminate duplicates in staging
        merge_sql = f"""
        MERGE `{DATASET_ID}.{table_name}` T
        USING (
            SELECT DISTINCT * FROM `{DATASET_ID}.{table_name}_staging`
        ) S
        ON {on_clause}
        WHEN MATCHED THEN
        UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
        INSERT ({insert_cols})
        VALUES ({insert_vals})
        """
        bigquery_client.query(merge_sql).result()
        print(f"MERGE upsert complete for {table_name} from staging.")

        # Now TRUNCATE staging
        truncate_sql = f"TRUNCATE TABLE `{DATASET_ID}.{table_name}_staging`"
        bigquery_client.query(truncate_sql).result()
        print(f"Truncated {table_name}_staging.\n")

    def verify_unique_records(self, table_name):
        unique_cols = UNIQUE_KEYS[table_name]
        if len(unique_cols) == 1:
            col_list = unique_cols[0]
            check_sql = f"""
            SELECT COUNT(*) as total_records,
                COUNT(DISTINCT {col_list}) as unique_records
            FROM `{DATASET_ID}.{table_name}`
            """
        else:
            col_list = ", ".join(unique_cols)
            check_sql = f"""
            SELECT COUNT(*) as total_records,
                COUNT(DISTINCT TO_JSON_STRING(STRUCT({col_list}))) as unique_records
            FROM `{DATASET_ID}.{table_name}`
            """
        
        results = list(bigquery_client.query(check_sql).result())
        total = results[0]["total_records"]
        unique = results[0]["unique_records"]
        
        if total > unique:
            print(f"WARNING: Found {total - unique} duplicates in {table_name} table!")
        else:
            print(f"Verified: No duplicates in {table_name} table.")
        
        return total == unique
    
    def execute(self, force_full_load=False, test_mode=False, max_pages=5000):
        """Main execution function with robust error handling and logging."""
        overall_start_time = time.time()
        run_id = str(uuid.uuid4())
        print(f"Starting ETL run {run_id} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Ensure required tables exist
            self.create_control_table_if_missing()
            self.create_final_and_staging_tables_if_missing()
            
            # Get the appropriate start date
            last_sync = None if force_full_load else self.get_last_sync_timestamp()
            start_date = last_sync or "2024-01-01T00:00:00+00:00"
            
            print(f"{'Full' if force_full_load else 'Incremental'} load starting from: {start_date}")
            print(f"Test mode: {test_mode}, Max pages: {max_pages}")
            
            # 1) Fetch data
            fetch_start = time.time()
            (
                orders,
                line_items,
                customers,
                shipping_addresses,
                discount_codes,
                marketing_consent
            ) = self.fetch_shopify_data(start_date=start_date, test_mode=test_mode, max_pages=max_pages)
            fetch_time = time.time() - fetch_start
            print(f"Data fetch completed in {fetch_time:.2f} seconds")
            
            # Capture order count for reporting
            orders_count = len(orders)
            if orders_count == 0:
                print("No new orders found since last sync.")
                self.update_sync_timestamp(0, status='success')
                print(f"ETL run completed successfully in {time.time() - overall_start_time:.2f} seconds")
                return
                
            # Proceed with loading tables
            try:
                # 2) Load each table to staging via GCS load jobs
                load_start = time.time()
                self.load_table_via_gcs("orders", orders)
                self.load_table_via_gcs("line_items", line_items)
                self.load_table_via_gcs("customers", customers)
                self.load_table_via_gcs("shipping_addresses", shipping_addresses)
                self.load_table_via_gcs("discount_codes", discount_codes)
                self.load_table_via_gcs("marketing_consent", marketing_consent)
                load_time = time.time() - load_start
                print(f"Data loading completed in {load_time:.2f} seconds")
                
                # 3) MERGE staging => final, then TRUNCATE
                merge_start = time.time()
                self.merge_staging_to_final("orders")
                self.merge_staging_to_final("line_items")
                self.merge_staging_to_final("customers")
                self.merge_staging_to_final("shipping_addresses")
                self.merge_staging_to_final("discount_codes")
                self.merge_staging_to_final("marketing_consent")
                merge_time = time.time() - merge_start
                print(f"Data merging completed in {merge_time:.2f} seconds")
                
                # 4) Update sync timestamp
                self.update_sync_timestamp(orders_count, status='success')
                
                total_time = time.time() - overall_start_time
                print(f"ETL run completed successfully in {total_time:.2f} seconds")
                
                # 5) Verify data (optional)
                if not test_mode:
                    self.verify_table_data()
                    
            except Exception as e:
                # If error occurs during load/merge, still record the run with error status
                print(f"Error during load or merge: {e}")
                print(traceback.format_exc())
                self.update_sync_timestamp(orders_count, status='error')
                raise
                
        except Exception as e:
            print(f"Critical error in ETL pipeline: {e}")
            print(traceback.format_exc())
            try:
                self.update_sync_timestamp(0, status='error')
            except:
                print("Failed to record error status in control table")
            raise

    def verify_table_data(self):
        """Verify data integrity after loading."""
        print("Performing data verification checks...")
        
        # 1. Check for duplicate keys in final tables
        for table_name in FINAL_TABLE_SCHEMAS.keys():
            self.verify_unique_records(table_name)
        
        # 2. Check referential integrity
        integrity_checks = [
            # Check that all line_items reference valid orders
            """
            SELECT COUNT(*) as invalid_refs
            FROM `{}.line_items` l
            LEFT JOIN `{}.orders` o ON l.order_id = o.order_id
            WHERE o.order_id IS NULL
            """.format(DATASET_ID, DATASET_ID),
            
            # Check that all shipping_addresses reference valid orders
            """
            SELECT COUNT(*) as invalid_refs
            FROM `{}.shipping_addresses` s
            LEFT JOIN `{}.orders` o ON s.order_id = o.order_id
            WHERE o.order_id IS NULL
            """.format(DATASET_ID, DATASET_ID)
        ]
        
        for check_sql in integrity_checks:
            results = list(bigquery_client.query(check_sql).result())
            invalid_count = results[0]["invalid_refs"]
            if invalid_count > 0:
                print(f"WARNING: Found {invalid_count} records with invalid references")
            else:
                print("Referential integrity check passed")
        
        print("Data verification completed")


if __name__ == "__main__":
    pipeline = ShopifyETLPipeline()
    # Example test: pipeline.execute(force_full_load=True, test_mode=False, max_pages=10)
    pipeline.execute(force_full_load=True, test_mode=False, max_pages=10)