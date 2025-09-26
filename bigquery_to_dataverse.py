import os
import json
import time
import uuid
import pytz
import logging
import random
from datetime import datetime
from typing import List, Tuple

import requests
import msal
from dotenv import load_dotenv
from google.cloud import bigquery

# ----------------------------
# Config & Logging
# ----------------------------
load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("bq2dv")

PROJECT = os.getenv("GOOGLE_PROJECT")
BQ_TABLE = os.getenv("BQ_TABLE", "crm_ds.customers")                  # dataset.table
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "5000"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
WATERMARK_FILE = os.getenv("WATERMARK_FILE", "watermark.json")
DEFAULT_WATERMARK = os.getenv("DEFAULT_WATERMARK", "2020-01-01T00:00:00Z")

DATAVERSE_URL = os.getenv("DATAVERSE_URL").rstrip("/")
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# Dataverse entity config (adjust to your table)
DV_ENTITY = "new_customers"     # logical name (not display name)
ALT_KEY_NAME = "externalid"     # alternate key attribute logical name

# Safety limits
MAX_RETRIES = 6                 # per HTTP call
BASE_BACKOFF = 2.0              # seconds
MAX_BATCH_PER_RUN = None        # set an int to hard-cap records per execution

# ----------------------------
# Watermark persistence
# ----------------------------
def load_watermark() -> str:
    if os.path.exists(WATERMARK_FILE):
        try:
            with open(WATERMARK_FILE, "r") as f:
                data = json.load(f)
                return data.get("last", DEFAULT_WATERMARK)
        except Exception as e:
            log.warning("Failed to read watermark file: %s", e)
    return DEFAULT_WATERMARK

def save_watermark(wm: str) -> None:
    tmp = WATERMARK_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"last": wm}, f)
    os.replace(tmp, WATERMARK_FILE)

# ----------------------------
# Auth: Dataverse token
# ----------------------------
def acquire_token() -> str:
    app = msal.ConfidentialClientApplication(
        client_id=CLIENT_ID,
        client_credential=CLIENT_SECRET,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}"
    )
    scope = [f"{DATAVERSE_URL}/.default"]
    result = app.acquire_token_for_client(scopes=scope)
    if "access_token" not in result:
        raise RuntimeError(f"Token acquisition failed: {result}")
    return result["access_token"]

# ----------------------------
# BigQuery fetch (paged by updated_at)
# ----------------------------
def fetch_rows_after(bq: bigquery.Client, watermark: str, limit: int) -> List[bigquery.table.Row]:
    sql = f"""
    SELECT externalid, name, email, phone, updated_at
    FROM `{PROJECT}.{BQ_TABLE}`
    WHERE updated_at > @last_watermark
    ORDER BY updated_at
    LIMIT {limit}
    """
    cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("last_watermark", "TIMESTAMP", watermark)
        ]
    )
    job = bq.query(sql, job_config=cfg)
    rows = list(job)
    return rows

# ----------------------------
# Mapping: BigQuery row -> (relative URL, body)
# ----------------------------
def map_row_to_request(row) -> Tuple[str, dict]:
    """
    Return:
      url_path: e.g., "new_customers(externalid='CUST001')"
      body:     JSON payload for PATCH
    Adjust mappings here for your target table.
    """
    # Dataverse columns (logical names)
    body = {
        "new_externalid": row.externalid,     # if your column logical name differs, change here
        "name": row.name,
        "emailaddress1": row.email,
        "telephone1": row.phone
    }

    # PATCH by alternate key (upsert)
    # If ext id contains quotes, escape single quotes per OData
    ext = str(row.externalid).replace("'", "''") if row.externalid is not None else ""
    url_path = f"{DV_ENTITY}({ALT_KEY_NAME}='{ext}')"
    return url_path, body

# ----------------------------
# HTTP helpers with retry/backoff
# ----------------------------
def rand_jitter():
    return random.uniform(0.0, 0.5)

def should_retry(status: int) -> bool:
    return status in (429, 500, 502, 503, 504)

def request_with_retry(method: str, url: str, headers: dict, data: bytes = None) -> requests.Response:
    attempt = 0
    while True:
        resp = requests.request(method, url, headers=headers, data=data)
        if resp.status_code in (200, 201, 202, 204):
            return resp

        if should_retry(resp.status_code) and attempt < MAX_RETRIES:
            attempt += 1
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                try:
                    wait = float(retry_after)
                except ValueError:
                    wait = BASE_BACKOFF * (2 ** (attempt - 1)) + rand_jitter()
            else:
                wait = BASE_BACKOFF * (2 ** (attempt - 1)) + rand_jitter()
            log.warning("HTTP %s -> %s. Retrying in %.2fs (attempt %d/%d)",
                        method, resp.status_code, wait, attempt, MAX_RETRIES)
            time.sleep(wait)
            continue

        # hard failure
        log.error("HTTP %s failed: %s %s", method, resp.status_code, resp.text[:500])
        resp.raise_for_status()

# ----------------------------
# Build a Dataverse $batch body
# ----------------------------
def build_batch_payload(changes: List[Tuple[str, dict]]) -> Tuple[str, bytes]:
    """
    changes: list of (url_path, body_json)
    Returns (boundary, payload_bytes)
    Uses a single change set for transactional upserts.
    """
    batch_boundary = f"batch_{uuid.uuid4()}"
    cs_boundary = f"changeset_{uuid.uuid4()}"

    parts = []
    # batch start
    parts.append(f"--{batch_boundary}\r\n")
    parts.append(f"Content-Type: multipart/mixed;boundary={cs_boundary}\r\n\r\n")

    for i, (url_path, body) in enumerate(changes):
        # one write request
        part = []
        part.append(f"--{cs_boundary}\r\n")
        part.append("Content-Type: application/http\r\n")
        part.append("Content-Transfer-Encoding: binary\r\n\r\n")
        # Note: absolute path required after /api/data/v9.2/
        part.append(f"PATCH {DATAVERSE_URL}/api/data/v9.2/{url_path} HTTP/1.1\r\n")
        part.append("Content-Type: application/json; charset=utf-8\r\n")
        # 'Prefer: return=representation' if you want the created/updated entity back
        part.append("Prefer: odata.include-annotations=*\r\n\r\n")
        part.append(json.dumps(body))
        part.append("\r\n")
        parts.append("".join(part))

    # end changeset
    parts.append(f"--{cs_boundary}--\r\n")
    # end batch
    parts.append(f"--{batch_boundary}--\r\n")

    payload = "".join(parts).encode("utf-8")
    return batch_boundary, payload

def post_batch(token: str, payload_boundary: str, payload_bytes: bytes):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": f"multipart/mixed;boundary={payload_boundary}",
        "Accept": "application/json"
    }
    url = f"{DATAVERSE_URL}/api/data/v9.2/$batch"
    return request_with_retry("POST", url, headers=headers, data=payload_bytes)

# ----------------------------
# Main processing loop
# ----------------------------
def run_once():
    if not all([PROJECT, DATAVERSE_URL, TENANT_ID, CLIENT_ID, CLIENT_SECRET]):
        raise RuntimeError("Missing required configuration (.env)")

    bq = bigquery.Client(project=PROJECT)
    token = acquire_token()
    log.info("Token acquired. Starting sync...")

    watermark = load_watermark()
    log.info("Loaded watermark: %s", watermark)

    total_processed = 0
    newest_wm = watermark

    while True:
        # Hard cap to limit per execution if configured
        if MAX_BATCH_PER_RUN and total_processed >= MAX_BATCH_PER_RUN:
            log.info("Reached MAX_BATCH_PER_RUN=%s, stopping.", MAX_BATCH_PER_RUN)
            break

        rows = fetch_rows_after(bq, newest_wm, PAGE_SIZE)
        if not rows:
            log.info("No more rows after %s. Done.", newest_wm)
            break

        log.info("Fetched %d rows from BigQuery (after %s).", len(rows), newest_wm)

        # Convert BigQuery row -> requests, track last updated_at
        changes: List[Tuple[str, dict]] = []
        page_latest = newest_wm
        for r in rows:
            url_path, body = map_row_to_request(r)
            changes.append((url_path, body))

            # updated_at can be datetime or string
            ru = r.updated_at
            if isinstance(ru, str):
                cand = ru
            else:
                # ensure UTC ISO format with Z
                if ru.tzinfo is None:
                    ru = ru.replace(tzinfo=pytz.UTC)
                cand = ru.isoformat().replace("+00:00", "Z")
            if cand > page_latest:
                page_latest = cand

        # Send in chunks to respect payload size & throttling
        for i in range(0, len(changes), BATCH_SIZE):
            chunk = changes[i:i+BATCH_SIZE]
            boundary, payload = build_batch_payload(chunk)
            log.info("Posting batch chunk %d-%d (%d records)...", i+1, i+len(chunk), len(chunk))
            resp = post_batch(token, boundary, payload)
            # Optional: parse multipart response for per-row diagnostics
            log.debug("Batch response status: %s", resp.status_code)

        newest_wm = page_latest
        total_processed += len(rows)
        save_watermark(newest_wm)
        log.info("Processed %d rows. Watermark advanced to %s.", total_processed, newest_wm)

        # If we got less than PAGE_SIZE, we're done
        if len(rows) < PAGE_SIZE:
            log.info("Final page processed.")
            break

    log.info("Sync complete. Total upserts: %d. Final watermark: %s", total_processed, newest_wm)

if __name__ == "__main__":
    try:
        run_once()
    except Exception as e:
        log.exception("Fatal error: %s", e)
        raise
