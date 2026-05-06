raw JSON → bronze Delta → silver Delta → gold Delta

## wistia_transform_delta.py

```python

import sys
import re
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    sha2,
    concat_ws,
    to_date
)

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_BUCKET"
    ]
)

S3_BUCKET = args["S3_BUCKET"]

s3_client = boto3.client("s3")


def get_latest_load_date(bucket):
    prefix = "raw/media_stats/"

    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        Delimiter="/"
    )

    load_dates = []

    for item in response.get("CommonPrefixes", []):
        folder = item["Prefix"]
        match = re.search(r"load_date=(\d{4}-\d{2}-\d{2})", folder)
        if match:
            load_dates.append(match.group(1))

    if not load_dates:
        raise Exception("No raw load_date partitions found in S3")

    return sorted(load_dates)[-1]


LOAD_DATE = get_latest_load_date(S3_BUCKET)

spark = (
    SparkSession.builder
    .appName("wistia_transform_delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

raw_path = f"s3://{S3_BUCKET}/raw/media_stats/load_date={LOAD_DATE}/"

bronze_path = f"s3://{S3_BUCKET}/bronze/media_stats/"
silver_path = f"s3://{S3_BUCKET}/silver/media_stats/"
gold_path = f"s3://{S3_BUCKET}/gold/fact_media_engagement/"

print(f"Starting transformation for LOAD_DATE={LOAD_DATE}")
print(f"Reading raw data from: {raw_path}")

raw_df = spark.read.option("multiline", "true").json(raw_path)

print("===== RAW DATA SCHEMA =====")
raw_df.printSchema()

print("===== RAW DATA COLUMNS =====")
print(raw_df.columns)

print("===== SAMPLE RAW DATA =====")
raw_df.show(5, truncate=False)

bronze_df = (
    raw_df
    .withColumn("load_date", lit(LOAD_DATE))
    .withColumn("ingestion_timestamp", current_timestamp())
)

print("Writing Bronze Delta table")

bronze_df.write.format("delta") \
    .mode("append") \
    .partitionBy("load_date") \
    .save(bronze_path)

silver_df = (
    bronze_df
    .dropDuplicates(["media_id"])
    .withColumn("processed_timestamp", current_timestamp())
)

print("Writing Silver Delta table")

silver_df.write.format("delta") \
    .mode("append") \
    .partitionBy("load_date") \
    .save(silver_path)

gold_df = (
    silver_df
    .select(
        col("media_id"),
        col("play_count"),
        col("play_rate"),
        col("engagement"),
        col("hours_watched"),
        col("visitors"),
        lit(LOAD_DATE).alias("load_date")
    )
    .withColumn("engagement_date", to_date(lit(LOAD_DATE)))
    .withColumn(
        "surrogate_key",
        sha2(
            concat_ws(
                "||",
                col("media_id").cast("string"),
                col("engagement_date").cast("string")
            ),
            256
        )
    )
    .withColumn("load_timestamp", current_timestamp())
)

print("===== GOLD DATA SCHEMA =====")
gold_df.printSchema()

print("===== SAMPLE GOLD DATA =====")
gold_df.show(5, truncate=False)

print("Writing Gold Delta table")

gold_df.write.format("delta") \
    .mode("append") \
    .partitionBy("load_date") \
    .save(gold_path)

print("Transformation completed successfully")

```


# Upload script to S3


s3://wistia-video-analytics-at/scripts/wistia_transform_delta.py

## Create Glue Job

- AWS Glue → ETL jobs → Create job → Script editor

- Choose:
    - Spark
- Job name:
    - wistia_transform_delta
- IAM role:
    - GlueExecutionRole-Wistia
- Glue version:
    - Glue 5.0 or 5.1
- Worker type:
    - G.1X
- Number of workers:
    - 2
- Timeout
    - 30 min
- Script path:
    - s3://wistia-video-analytics-at/scripts/wistia_transform_delta.py
- Add job parameters
    - Add:
        - --S3_BUCKET
        - wistia-video-analytics-at
        - --datalake-formats
        - delta
        - --conf
        - spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog


AWS Glue Job – Execution Progress

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Wistia-Video-Analytics/main/Phases/Ref%20Files/Glue%20job%20progress.png" width="750"/> </p>

Amazon Athena – Analytics Tables

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Wistia-Video-Analytics/main/Phases/Ref%20Files/Athena%20tables.png" width="750"/> </p>

Initial Data Load – First Pipeline Execution

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Wistia-Video-Analytics/main/Phases/Ref%20Files/first%20load.png" width="750"/> </p>

Incremental Data Load – Second Pipeline Execution

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Wistia-Video-Analytics/main/Phases/Ref%20Files/second%20load.png" width="750"/> </p>

