# Python-BigQuery-Dataverse-WebAPI
BigQuery → Dataverse Web API


1) Install & prepare
python -m venv venv
source venv/bin/activate

pip install google-cloud-bigquery msal requests python-dotenv pytz


Create a .env file:

# --- BigQuery ---
GOOGLE_PROJECT=your-gcp-project-id
# Optional: dataset.table if you want to override in code
BQ_TABLE=crm_ds.customers

# --- Dataverse / Dynamics 365 CE ---
DATAVERSE_URL=https://yourorg.crm.dynamics.com
TENANT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
CLIENT_SECRET=your-client-secret

# --- App behavior (optional; these have safe defaults) ---
PAGE_SIZE=5000            # BigQuery rows per pull
BATCH_SIZE=50             # Upserts per Dataverse batch ($batch payload)
WATERMARK_FILE=watermark.json
DEFAULT_WATERMARK=2020-01-01T00:00:00Z
LOG_LEVEL=INFO


Export your GCP service account key:

export GOOGLE_APPLICATION_CREDENTIALS="/path/to/sa-key.json"


#Use cron/systemd to schedule. Example (hourly):

crontab -e
0 * * * * /path/to/venv/bin/python /path/to/bigquery_to_dataverse.py >> /path/to/sync.log 2>&1
