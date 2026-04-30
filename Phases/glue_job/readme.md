
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
MEDIA_IDS = args["MEDIA_IDS"].split(",")

s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager")


CHECKPOINT_KEY = "state/checkpoint.json"


def get_secret_token(secret_name):
    response = secrets.get_secret_value(SecretId=secret_name)
    secret_value = json.loads(response["SecretString"])
    return secret_value["api_token"]


def read_checkpoint():
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=CHECKPOINT_KEY)
        checkpoint = json.loads(response["Body"].read().decode("utf-8"))
        return checkpoint.get("last_successful_run_utc", "1970-01-01T00:00:00Z")
    except Exception:
        return "1970-01-01T00:00:00Z"


def write_checkpoint(run_time_utc):
    checkpoint = {
        "last_successful_run_utc": run_time_utc
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


def call_wistia_api(media_id, api_token):
    url = f"https://api.wistia.com/v1/stats/medias/{media_id}.json"

    headers = {
        "Authorization": f"Bearer {api_token}"
    }

    response = requests.get(url, headers=headers, timeout=60)

    if response.status_code != 200:
        raise Exception(
            f"Wistia API failed for media_id={media_id}. "
            f"Status={response.status_code}, Response={response.text}"
        )

    return response.json()


def main():
    print("Starting Wistia API ingestion job")

    api_token = get_secret_token(SECRET_NAME)
    last_checkpoint = read_checkpoint()

    run_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    load_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    print(f"Last checkpoint: {last_checkpoint}")
    print(f"Current run time: {run_time_utc}")

    for media_id in MEDIA_IDS:
        print(f"Processing media_id: {media_id}")

        media_stats = call_wistia_api(media_id, api_token)

        s3_key = (
            f"raw/media_stats/"
            f"load_date={load_date}/"
            f"media_id={media_id}/"
            f"media_stats.json"
        )

        write_json_to_s3(media_stats, s3_key)

        print(f"Saved raw media stats to s3://{S3_BUCKET}/{s3_key}")

    write_checkpoint(run_time_utc)

    print("Checkpoint updated successfully")
    print("Wistia API ingestion job completed")


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


