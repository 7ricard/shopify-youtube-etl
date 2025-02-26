# Monitoring & Recovery Strategy for Shopify ETL Pipeline

## Monitoring Metrics to Track

1. **Pipeline Execution Metrics**
   - Run frequency & completion status
   - Execution time (total and by stage)
   - Records processed per run
   - Error rate and types

2. **Data Quality Metrics**
   - Record counts by table
   - Missing values in critical fields
   - Referential integrity violations
   - Duplicate detection

3. **Resource Usage**
   - BigQuery slots consumption
   - Storage usage
   - API rate limit utilization

## Recommended GCP Monitoring Setup

### Custom Dashboard

Create a custom Cloud Monitoring dashboard with these panels:

1. **Pipeline Health**
   - Success/failure count by day
   - Average execution time
   - Records processed trend

2. **Data Quality**
   - Missing values count
   - Integrity violations count
   - Daily growth rate of each table

3. **Resource Usage**
   - BigQuery slot utilization
   - Storage growth rate
   - API calls per minute

### Alerting Policy Recommendations

1. **Critical Alerts** (require immediate action)
   - Pipeline failures
   - >5% referential integrity violations
   - >2 consecutive missed runs

2. **Warning Alerts** (monitor closely)
   - Execution time >25% above baseline
   - Record count <50% of daily average
   - API rate limit threshold approaching (>80%)

## Recovery Procedures

### 1. For Pipeline Failures

**Light Recovery**: Re-trigger the Cloud Function with the same parameters

```bash
curl -X POST \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d '{"force_full_load": false}' \
  https://us-central1-YOUR_PROJECT_ID.cloudfunctions.net/shopify-etl-function
```

**Full Recovery**: Trigger a full load from a safe date

```bash
# Identify last successful sync date from the control table
bq query --nouse_legacy_sql \
'SELECT FORMAT_TIMESTAMP("%Y-%m-%d", last_sync_timestamp) as last_success 
 FROM `pipeline_metadata.sync_control` 
 WHERE status="success" 
 ORDER BY last_sync_timestamp DESC LIMIT 1'

# Trigger full load from before that date (e.g., 1 day before)
curl -X POST \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d '{"force_full_load": true}' \
  https://us-central1-YOUR_PROJECT_ID.cloudfunctions.net/shopify-etl-function
```

### 2. For Data Quality Issues

1. Query the problematic records:
   ```sql
   -- Example: Find order_ids with missing line items
   SELECT o.order_id
   FROM `Shopify.orders` o
   LEFT JOIN `Shopify.line_items` l ON o.order_id = l.order_id
   WHERE l.order_id IS NULL;
   ```

2. Reprocess specific orders:
   - Create a temp table with problematic IDs
   - Modify your pipeline to accept a list of specific order_ids
   - Trigger a targeted reload

### 3. Disaster Recovery

If the pipeline breaks completely or data is corrupted:

1. Stop Cloud Scheduler jobs:
   ```bash
   gcloud scheduler jobs pause shopify-etl-hourly
   ```

2. Back up current data (optional):
   ```bash
   # Export tables to GCS
   bq extract --destination_format=NEWLINE_DELIMITED_JSON \
     Shopify.orders gs://my-shopify-bucket/backup/orders_$(date +%Y%m%d).json
   ```

3. Perform a full historical load:
   ```bash
   curl -X POST \
     -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
     -H "Content-Type: application/json" \
     -d '{"force_full_load": true, "max_pages": 10000}' \
     https://us-central1-YOUR_PROJECT_ID.cloudfunctions.net/shopify-etl-function
   ```

4. Resume Cloud Scheduler:
   ```bash
   gcloud scheduler jobs resume shopify-etl-hourly
   ```
