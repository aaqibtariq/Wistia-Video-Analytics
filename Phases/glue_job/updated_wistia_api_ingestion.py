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

CHECKPOINT_KEY = "state/checkpoint.json"

s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager")


def get_secret_token(secret_name):
    response = secrets.get_secret_value(SecretId=secret_name)
    secret_value = json.loads(response["SecretString"])
    return secret_value["api_token"]


def read_checkpoint():
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=CHECKPOINT_KEY)
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


def make_get_request(url, api_token, params=None):
    headers = {
        "Authorization": f"Bearer {api_token}"
    }

    response = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=60
    )

    if response.status_code != 200:
        raise Exception(
            f"API request failed. "
            f"URL={url}, Status={response.status_code}, Response={response.text}"
        )

    return response.json()


def call_paginated_api(base_url, api_token, base_params=None, page_size=100):
    """
    Generic pagination helper.

    This supports common page/per_page style pagination.
    If the endpoint does not support pagination, it still returns page 1 safely.
    """

    all_records = []
    page = 1

    while True:
        params = dict(base_params or {})
        params["page"] = page
        params["per_page"] = page_size

        print(f"Calling paginated API: url={base_url}, page={page}")

        data = make_get_request(
            url=base_url,
            api_token=api_token,
            params=params
        )

        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = (
                data.get("data")
                or data.get("items")
                or data.get("visitors")
                or data.get("events")
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


def get_media_stats(media_id, api_token):
    url = f"https://api.wistia.com/v1/stats/medias/{media_id}.json"

    return make_get_request(
        url=url,
        api_token=api_token
    )


def get_media_metadata(media_id, api_token):
    """
    Metadata endpoint can vary by Wistia account/API permissions.
    This is separated so failure does not break the whole ingestion.
    """

    url = f"https://api.wistia.com/v1/medias/{media_id}.json"

    try:
        return make_get_request(
            url=url,
            api_token=api_token
        )
    except Exception as e:
        print(f"Media metadata API failed for media_id={media_id}: {str(e)}")

        return {
            "media_id": media_id,
            "metadata_error": str(e)
        }


def get_media_visitors_exploration(media_id, api_token):
    """
    Visitor-level data discovery.

    We are saving the response raw first because Wistia visitor endpoints/fields
    depend on API permissions and response structure.

    After we inspect this output, we will build dim_visitor and fact_visitor_engagement.
    """

    possible_urls = [
        f"https://api.wistia.com/v1/stats/medias/{media_id}/visitors.json",
        f"https://api.wistia.com/v1/stats/medias/{media_id}/engagement.json",
        f"https://api.wistia.com/v1/stats/medias/{media_id}/events.json"
    ]

    results = []

    for url in possible_urls:
        try:
            print(f"Trying visitor-level endpoint: {url}")

            records = call_paginated_api(
                base_url=url,
                api_token=api_token,
                base_params=None,
                page_size=100
            )

            results.append({
                "media_id": media_id,
                "endpoint": url,
                "status": "success",
                "record_count": len(records),
                "records": records[:500]
            })

            print(f"Visitor endpoint success: {url}, records={len(records)}")

        except Exception as e:
            print(f"Visitor endpoint failed: {url}, error={str(e)}")

            results.append({
                "media_id": media_id,
                "endpoint": url,
                "status": "failed",
                "error": str(e)
            })

    return results


def main():
    print("Starting Wistia API ingestion job")

    api_token = get_secret_token(SECRET_NAME)
    checkpoint = read_checkpoint()

    last_checkpoint = checkpoint["last_successful_run_utc"]

    run_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    load_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    print(f"Last checkpoint: {last_checkpoint}")
    print(f"Current run time: {run_time_utc}")
    print(f"Current load_date: {load_date}")

    for media_id in MEDIA_IDS:
        media_id = media_id.strip()

        print(f"Processing media_id: {media_id}")

        # 1. Media-level engagement stats
        media_stats = get_media_stats(media_id, api_token)
        media_stats["media_id"] = media_id
        media_stats["load_date"] = load_date
        media_stats["ingested_at_utc"] = run_time_utc

        print("===== MEDIA STATS RESPONSE KEYS =====")
        print(media_stats.keys())

        media_stats_key = (
            f"raw/media_stats/"
            f"load_date={load_date}/"
            f"media_id={media_id}/"
            f"media_stats.json"
        )

        write_json_to_s3(media_stats, media_stats_key)

        print(f"Saved media stats to s3://{S3_BUCKET}/{media_stats_key}")

        # 2. Media metadata
        media_metadata = get_media_metadata(media_id, api_token)
        media_metadata["media_id"] = media_id
        media_metadata["load_date"] = load_date
        media_metadata["ingested_at_utc"] = run_time_utc

        print("===== MEDIA METADATA RESPONSE KEYS =====")
        if isinstance(media_metadata, dict):
            print(media_metadata.keys())

        media_metadata_key = (
            f"raw/media_metadata/"
            f"load_date={load_date}/"
            f"media_id={media_id}/"
            f"media_metadata.json"
        )

        write_json_to_s3(media_metadata, media_metadata_key)

        print(f"Saved media metadata to s3://{S3_BUCKET}/{media_metadata_key}")

        # 3. Visitor-level exploration
        visitor_data = get_media_visitors_exploration(media_id, api_token)

        visitor_key = (
            f"raw/visitor_stats/"
            f"load_date={load_date}/"
            f"media_id={media_id}/"
            f"visitor_stats_exploration.json"
        )

        write_json_to_s3(visitor_data, visitor_key)

        print(f"Saved visitor exploration to s3://{S3_BUCKET}/{visitor_key}")

    write_checkpoint(run_time_utc, load_date)

    print("Checkpoint updated successfully")
    print("Wistia API ingestion job completed")


if __name__ == "__main__":
    main()