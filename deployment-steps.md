# Shopify ETL Pipeline Deployment Guide

## 1. Code Structure & Preparation

Organize your code in the following structure:

```
shopify-etl/
├── main.py              # Cloud Function entry point
├── shopify_etl.py       # Your main ETL code
├── requirements.txt     # Dependencies
└── README.md            # Documentation
```

### requirements.txt

```
google-cloud-bigquery>=3.0.0
google-cloud-storage>=2.0.0
google-auth>=2.0.0
requests>=2.25.0
functions-framework>=3.0.0
```

## 2. Set Up Google Cloud Environment

### Create Service Account

1. Go to IAM & Admin > Service Accounts
2. Create a new service account named `shopify-etl-sa`
3. Grant these roles:
   - BigQuery Data Editor
   - BigQuery Job User
   - Storage Object Admin
   - Logs Writer

### Create Cloud Storage Bucket

```bash
# Create GCS bucket for staging data
gsutil mb -l us-central1 gs://my-shopify-bucket/
```

### Create BigQuery Dataset

```bash
# Create the main dataset
bq mk --location=US Shopify

# Create the metadata dataset
bq mk --location=US pipeline_metadata
```

## 3. Deploy to Cloud Functions

```bash
# Navigate to your code directory
cd shopify-etl

# Deploy the Cloud Function
gcloud functions deploy shopify-etl-function \
  --gen2 \
  --runtime=python310 \
  --region=us-central1 \
  --source=. \
  --entry-point=shopify_etl_function \
  --trigger-http \
  --timeout=540s \
  --memory=2048MB \
  --service-account=shopify-etl-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com \
  --set-env-vars="GCS_BUCKET_NAME=my-shopify-bucket,SHOPIFY_STORE=dj4qxk-yt,SHOPIFY_API_VERSION=2024-01"
```

**Security Note**: Set sensitive credentials as secret environment variables:

```bash
# Create secrets
gcloud secrets create shopify-api-key --replication-policy="automatic"
gcloud secrets create shopify-password --replication-policy="automatic"

# Set secret values
echo -n "5dd52f10564583d30a3a0eccfbb47bb9" | gcloud secrets versions add shopify-api-key --data-file=-
echo -n "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" | gcloud secrets versions add shopify-password --data-file=-

# Grant access to service account
gcloud secrets add-iam-policy-binding shopify-api-key \
  --member="serviceAccount:shopify-etl-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud secrets add-iam-policy-binding shopify-password \
  --member="serviceAccount:shopify-etl-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

# Update your code to access secrets
# Add this to your ShopifyETLPipeline __init__ method:
# from google.cloud import secretmanager
# client = secretmanager.SecretManagerServiceClient()
# API_KEY = client.access_secret_version(request={"name": "projects/YOUR_PROJECT_ID/secrets/shopify-api-key/versions/latest"}).payload.data.decode("UTF-8")
# PASSWORD = client.access_secret_version(request={"name": "projects/YOUR_PROJECT_ID/secrets/shopify-password/versions/latest"}).payload.data.decode("UTF-8")
```

## 4. Set Up Cloud Scheduler

```bash
# Create a scheduler job to run every hour
gcloud scheduler jobs create http shopify-etl-hourly \
  --schedule="0 * * * *" \
  --uri="https://us-central1-YOUR_PROJECT_ID.cloudfunctions.net/shopify-etl-function" \
  --http-method=POST \
  --message-body='{"force_full_load": false, "test_mode": false, "max_pages": 5000}' \
  --headers="Content-Type=application/json" \
  --oidc-service-account-email=shopify-etl-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com \
  --oidc-token-audience="https://us-central1-YOUR_PROJECT_ID.cloudfunctions.net/shopify-etl-function"
```

## 5. Monitoring & Alerting

### Set up logging alerts

1. Go to Logging > Logs Explorer
2. Create a query for errors:
   ```
   resource.type="cloud_function"
   resource.labels.function_name="shopify-etl-function"
   severity>=ERROR
   ```
3. Click "Create Alert"
4. Set up notifications (email, Slack, PagerDuty, etc.)

### Set up uptime check

1. Go to Monitoring > Uptime Checks
2. Create a new uptime check for your Cloud Function
3. Set frequency to 10-15 minutes
4. Configure alerts if the check fails

## 6. Connect to Looker

1. In BigQuery, create views for Looker:

```sql
CREATE OR REPLACE VIEW `Shopify.dashboard_orders_view` AS
SELECT 
  o.order_id,
  o.created_at,
  o.updated_at,
  o.total_price,
  o.financial_status,
  o.fulfillment_status,
  o.currency,
  o.source_name,
  c.first_name AS customer_first_name,
  c.last_name AS customer_last_name,
  c.email AS customer_email
FROM 
  `Shopify.orders` o
LEFT JOIN
  `Shopify.customers` c ON o.customer_id = c.customer_id;
```

2. In Looker:
   - Connect to BigQuery using a service account
   - Create a new model pointing to your views
   - Design dashboards based on your views
