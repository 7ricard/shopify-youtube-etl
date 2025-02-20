import google.auth
from googleapiclient.discovery import build
from google.cloud import bigquery
from google_auth_oauthlib.flow import InstalledAppFlow

def get_authenticated_service():
    """Authenticate with YouTube Analytics API using manual OAuth."""
    flow = InstalledAppFlow.from_client_secrets_file(
        "youtube_oauth.json",  # Ensure this is the correct file path
        scopes=["https://www.googleapis.com/auth/yt-analytics.readonly", "https://www.googleapis.com/auth/youtube.readonly"]
    )
    credentials = flow.run_local_server(port=8080, prompt="consent")

    # Save refresh token for future use
    with open("youtube_token.json", "w") as token:
        token.write(credentials.to_json())

    return build("youtubeAnalytics", "v2", credentials=credentials)

# BigQuery Setup
client = bigquery.Client()
DATASET_ID = "marketing_insights"
TABLE_ID = "youtube_analytics_raw"

def fetch_youtube_data():
    """Fetch YouTube Analytics data and insert into BigQuery."""
    youtube = get_authenticated_service()

    request = youtube.reports().query(
        ids="channel==UCIRgNZikvXfDX1DoO6BWduw",
        startDate="2024-02-01",
        endDate="2024-02-15",
        metrics="views,likes,comments,shares,estimatedRevenue",
        dimensions="day"
    ).execute()

    rows = []
    for row in request.get("rows", []):
        rows.append({
            "date": row[0],
            "views": int(row[1]),
            "likes": int(row[2]),
            "comments": int(row[3]),
            "shares": int(row[4]),
            "revenue": float(row[5])
        })

    # Insert data into BigQuery
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    errors = client.insert_rows_json(table_ref, rows)

    if errors:
        print(f"BigQuery insertion errors: {errors}")
    else:
        print(f"Successfully inserted {len(rows)} rows into {TABLE_ID}")

if __name__ == "__main__":
    fetch_youtube_data()