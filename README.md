# Shopify & YouTube Data Pipeline (Python + BigQuery)

A fully automated cloud-based data pipeline that ingests data from Shopify and YouTube APIs into BigQuery for unified analysis across eCommerce and marketing.

---

## What This Project Does

- Pulls order, customer, and product data from **Shopify’s REST API**
- Fetches engagement and revenue metrics from **YouTube Analytics**
- Cleans, normalizes, and loads the data into **BigQuery**
- Enables centralized reporting and downstream dashboarding
- Supports **incremental syncs**, **deduplication**, and **automated execution**

---

## Features

- Modular Python code using best practices
- Automated Cloud Function trigger for Shopify ETL
- Exponential backoff, rate-limit handling, and full pagination support
- Google Cloud Storage staging + BigQuery load jobs
- Control table with sync tracking and metadata logging
- YouTube OAuth flow + metrics ingestion by day

---

## Tech Stack

- **Languages:** Python  
- **Cloud:** Google Cloud Functions, BigQuery, GCS  
- **Auth:** Basic Auth (Shopify), OAuth 2.0 (YouTube API)  
- **Libraries:** `requests`, `google-cloud-bigquery`, `google-cloud-storage`, `google-auth`

---

## Components

| File                  | Description |
|-----------------------|-------------|
| `main.py`             | Cloud Function entry point to trigger the Shopify pipeline |
| `shopify_etl.py`      | Full Shopify pipeline logic with table creation, deduping, merge logic |
| `fetch_youtube_data.py` | Script to pull YouTube metrics and load into BigQuery |

---

## Use Cases Enabled

- Cross-channel analysis of content → engagement → sales  
- ROAS & campaign effectiveness tracking  
- Unified performance view across orders and marketing  
- Automation-friendly data flow into dashboards or models

---

## How to Run

**Shopify Pipeline:**
```bash
# Trigger via Cloud Function (or manually)
python main.py  # or deploy via GCP
```

## **Youtube Ingestion:**
### Run manually for token flow, then scheduled (or trigger with refresh token logic)
python fetch_youtube_data.py

**Running Locally with Docker:**
You can also run this project inside a containerized environment using Docker:
```bash
# Build the Docker image
docker build -t shopify-youtube-etl .

# Run the container
docker run --rm shopify-youtube-etl
```

--
🔒 Note on Data Privacy

This pipeline was built for a real eCommerce client. Code and process are shared publicly; data and credentials are confidential.