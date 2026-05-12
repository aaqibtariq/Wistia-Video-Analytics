

# Phase 2: Glue Ingestion Job 

We will build:

- API call (Wistia)
- Pagination
- Incremental logic
- checkpoint read/write
- Raw JSON write to S3


# Glue job wistia_api_ingestion.py


```python

import sys
import json
import boto3
import requests
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_BUCKET",
        "SECRET_NAME",
        "MEDIA_IDS"
    ]
)

S3_BUCKET = args["S3_BUCKET"]
SECRET_NAME = args["SECRET_NAME"]
MEDIA_IDS = [m.strip() for m in args["MEDIA_IDS"].split(",")]

CHECKPOINT_KEY = "state/checkpoint.json"
WISTIA_API_VERSION = "2026-03"

s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager")


# ============================================================
# Helpers
# ============================================================

def get_secret_token(secret_name):
    response = secrets.get_secret_value(SecretId=secret_name)
    secret_value = json.loads(response["SecretString"])
    return secret_value["api_token"]


def read_checkpoint():
    try:
        response = s3.get_object(
            Bucket=S3_BUCKET,
            Key=CHECKPOINT_KEY
        )

        checkpoint = json.loads(response["Body"].read().decode("utf-8"))

        return {
            "last_successful_run_utc": checkpoint.get(
                "last_successful_run_utc",
                "1970-01-01T00:00:00Z"
            ),
            "last_load_date": checkpoint.get(
                "last_load_date",
                "1970-01-01"
            )
        }

    except Exception as e:
        print(f"Checkpoint not found or unreadable. Starting fresh. Error: {str(e)}")

        return {
            "last_successful_run_utc": "1970-01-01T00:00:00Z",
            "last_load_date": "1970-01-01"
        }


def write_checkpoint(run_time_utc, load_date):
    checkpoint = {
        "last_successful_run_utc": run_time_utc,
        "last_load_date": load_date
    }

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=CHECKPOINT_KEY,
        Body=json.dumps(checkpoint, indent=2),
        ContentType="application/json"
    )


def write_json_to_s3(data, key):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(data, indent=2),
        ContentType="application/json"
    )


def make_get_request(url, api_token, params=None, modern=False):
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Accept": "application/json"
    }

    if modern:
        headers["X-Wistia-API-Version"] = WISTIA_API_VERSION

    response = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=60
    )

    if response.status_code != 200:
        raise Exception(
            f"API request failed. "
            f"URL={url}, "
            f"Status={response.status_code}, "
            f"Response={response.text[:1000]}"
        )

    return response.json()


def call_paginated_api(base_url, api_token, modern=False, page_size=100):
    all_records = []
    page = 1

    while True:
        params = {
            "page": page,
            "per_page": page_size
        }

        print(f"Calling paginated API: url={base_url}, page={page}")

        data = make_get_request(
            url=base_url,
            api_token=api_token,
            params=params,
            modern=modern
        )

        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = (
                data.get("data")
                or data.get("items")
                or data.get("results")
                or data.get("medias")
                or []
            )
        else:
            records = []

        all_records.extend(records)

        print(f"Page {page} returned {len(records)} records")

        if len(records) < page_size:
            break

        page += 1

    return all_records


# ============================================================
# Wistia API Calls
# ============================================================

def get_media_stats_v1(media_id, api_token):
    url = f"https://api.wistia.com/v1/stats/medias/{media_id}.json"

    return make_get_request(
        url=url,
        api_token=api_token
    )


def get_media_metadata_v1(media_id, api_token):
    url = f"https://api.wistia.com/v1/medias/{media_id}.json"

    return make_get_request(
        url=url,
        api_token=api_token
    )


def get_modern_media_engagement(media_id, api_token):
    """
    Modern Wistia engagement endpoint.

    Returns:
    - engagement
    - engagement_data
    - rewatch_data

    This does not expose visitor_id/ip-level data for the provided token,
    but it gives detailed second-level engagement curve metrics.
    """

    url = f"https://api.wistia.com/modern/stats/medias/{media_id}/engagement"

    return make_get_request(
        url=url,
        api_token=api_token,
        modern=True
    )


def get_modern_media_by_date(media_id, api_token):
    """
    Modern Wistia date-level stats endpoint.
    """

    url = f"https://api.wistia.com/modern/stats/medias/{media_id}/by_date"

    return make_get_request(
        url=url,
        api_token=api_token,
        modern=True
    )


def get_modern_aggregated_stats(media_id, api_token):
    """
    Modern Wistia aggregated stats endpoint.
    """

    url = f"https://api.wistia.com/modern/medias/{media_id}/stats"

    return make_get_request(
        url=url,
        api_token=api_token,
        modern=True
    )


def get_modern_media_inventory(api_token):
    """
    Modern media inventory endpoint with pagination.
    Used to prove pagination and collect account-level media inventory.
    """

    url = "https://api.wistia.com/modern/medias"

    return call_paginated_api(
        base_url=url,
        api_token=api_token,
        modern=True,
        page_size=100
    )


# ============================================================
# Main
# ============================================================

def main():
    print("Starting final Wistia API ingestion job")

    api_token = get_secret_token(SECRET_NAME)
    checkpoint = read_checkpoint()

    run_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    load_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    print(f"Last checkpoint: {checkpoint.get('last_successful_run_utc')}")
    print(f"Current run time: {run_time_utc}")
    print(f"Current load_date: {load_date}")

    # --------------------------------------------------------
    # Account-level media inventory with pagination
    # --------------------------------------------------------

    try:
        media_inventory = get_modern_media_inventory(api_token)

        media_inventory_key = (
            f"raw/media_inventory/"
            f"load_date={load_date}/"
            f"media_inventory.json"
        )

        media_inventory_payload = {
            "load_date": load_date,
            "ingested_at_utc": run_time_utc,
            "record_count": len(media_inventory),
            "records": media_inventory
        }

        write_json_to_s3(media_inventory_payload, media_inventory_key)

        print(
            f"Saved media inventory to "
            f"s3://{S3_BUCKET}/{media_inventory_key}"
        )

    except Exception as e:
        print(f"Media inventory ingestion failed but pipeline will continue. Error: {str(e)}")

    # --------------------------------------------------------
    # Media-level ingestion for required media IDs
    # --------------------------------------------------------

    for media_id in MEDIA_IDS:
        print(f"Processing media_id: {media_id}")

        # 1. V1 Media-level engagement stats
        media_stats = get_media_stats_v1(media_id, api_token)
        media_stats["media_id"] = media_id
        media_stats["load_date"] = load_date
        media_stats["ingested_at_utc"] = run_time_utc

        media_stats_key = (
            f"raw/media_stats/"
            f"load_date={load_date}/"
            f"media_id={media_id}/"
            f"media_stats.json"
        )

        write_json_to_s3(media_stats, media_stats_key)

        print(
            f"Saved media stats to "
            f"s3://{S3_BUCKET}/{media_stats_key}"
        )

        # 2. V1 Media metadata
        media_metadata = get_media_metadata_v1(media_id, api_token)
        media_metadata["media_id"] = media_id
        media_metadata["load_date"] = load_date
        media_metadata["ingested_at_utc"] = run_time_utc

        media_metadata_key = (
            f"raw/media_metadata/"
            f"load_date={load_date}/"
            f"media_id={media_id}/"
            f"media_metadata.json"
        )

        write_json_to_s3(media_metadata, media_metadata_key)

        print(
            f"Saved media metadata to "
            f"s3://{S3_BUCKET}/{media_metadata_key}"
        )

        # 3. Modern engagement curve
        modern_engagement = get_modern_media_engagement(media_id, api_token)
        modern_engagement["media_id"] = media_id
        modern_engagement["load_date"] = load_date
        modern_engagement["ingested_at_utc"] = run_time_utc

        modern_engagement_key = (
            f"raw/media_engagement_curve/"
            f"load_date={load_date}/"
            f"media_id={media_id}/"
            f"media_engagement_curve.json"
        )

        write_json_to_s3(modern_engagement, modern_engagement_key)

        print(
            f"Saved modern engagement curve to "
            f"s3://{S3_BUCKET}/{modern_engagement_key}"
        )

        # 4. Modern daily stats
        modern_by_date = get_modern_media_by_date(media_id, api_token)

        modern_by_date_payload = {
            "media_id": media_id,
            "load_date": load_date,
            "ingested_at_utc": run_time_utc,
            "records": modern_by_date
        }

        modern_by_date_key = (
            f"raw/media_daily_stats/"
            f"load_date={load_date}/"
            f"media_id={media_id}/"
            f"media_daily_stats.json"
        )

        write_json_to_s3(modern_by_date_payload, modern_by_date_key)

        print(
            f"Saved modern daily stats to "
            f"s3://{S3_BUCKET}/{modern_by_date_key}"
        )

        # 5. Modern aggregated stats
        modern_aggregated = get_modern_aggregated_stats(media_id, api_token)
        modern_aggregated["media_id"] = media_id
        modern_aggregated["load_date"] = load_date
        modern_aggregated["ingested_at_utc"] = run_time_utc

        modern_aggregated_key = (
            f"raw/media_aggregated_stats/"
            f"load_date={load_date}/"
            f"media_id={media_id}/"
            f"media_aggregated_stats.json"
        )

        write_json_to_s3(modern_aggregated, modern_aggregated_key)

        print(
            f"Saved modern aggregated stats to "
            f"s3://{S3_BUCKET}/{modern_aggregated_key}"
        )

    write_checkpoint(run_time_utc, load_date)

    print("Checkpoint updated successfully")
    print("Final Wistia API ingestion job completed successfully")


if __name__ == "__main__":
    main()

```

- Read Wistia API token from Secrets Manager
- Read checkpoint.json from S3
- Call Wistia API for both media IDs
- Handle pagination
- Save raw JSON to S3
- Update checkpoint.json after success

- Upload script to S3
  - Upload this file to:
    - s3://wistia-video-analytics-at/scripts/wistia_api_ingestion.py


  # Glue JOb Creation


-  Go to:

-  AWS Glue → ETL jobs → Script editor

-  Choose:

  -  Spark or Python shell script editor

-  For this ingestion job, choose:

  -  Python Shell

-  Job name:

  -  wistia_api_ingestion

-  IAM Role:

  -  GlueExecutionRole-Wistia

-  Script path:

  -  s3://wistia-video-analytics-at/scripts/wistia_api_ingestion.py
  - 
-  Add Job Parameters
-  Under Job details → Advanced properties → Job parameters, add:
-  --S3_BUCKET
    -  wistia-video-analytics-at
-  --SECRET_NAME
    -  wistia/api_token
-  --MEDIA_IDS
    -  ****, ***
-  Run Job
-  Click:
-  Run

AWS Glue Job Creation – Step 1

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Wistia-Video-Analytics/main/Phases/Ref%20Files/glue%20job%20creation%201.png" width="750"/> </p>

AWS Glue Job Creation – Step 2

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Wistia-Video-Analytics/main/Phases/Ref%20Files/glue%20job%20creation%202.png" width="750"/> </p>

AWS Glue Job Creation – Step 3

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Wistia-Video-Analytics/main/Phases/Ref%20Files/glue%20job%20creation%203.png" width="750"/> </p>

AWS Glue Job Creation – Step 4

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Wistia-Video-Analytics/main/Phases/Ref%20Files/glue%20job%20creation%204.png" width="750"/> </p>

AWS Glue Job Execution Status

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Wistia-Video-Analytics/main/Phases/Ref%20Files/glue%20job%20status%20.png" width="750"/> </p>

S3 Checkpoint Mechanism – State Tracking

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Wistia-Video-Analytics/main/Phases/Ref%20Files/S3%20checkpoint.png" width="750"/> </p>

Checkpoint Updates – Incremental Processing

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Wistia-Video-Analytics/main/Phases/Ref%20Files/checkpoint%20update%20.png" width="750"/> </p>

Media ID Tracking – Incremental Load Logic

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Wistia-Video-Analytics/main/Phases/Ref%20Files/s3%20media%20id%20update.png" width="750"/> </p>


-  Glue Python Shell job ran successfully
-   Wistia API token read from Secrets Manager
-   checkpoint.json read and updated
-   Both media IDs processed
-  Raw JSON written to S3

