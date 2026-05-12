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
    to_date,
    input_file_name,
    regexp_extract,
    explode,
    posexplode,
    when
)


# ============================================================
# Glue Job Arguments
# ============================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_BUCKET"
    ]
)

S3_BUCKET = args["S3_BUCKET"]

s3_client = boto3.client("s3")


# ============================================================
# Get Latest Load Date Dynamically
# ============================================================

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


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("wistia_transform_delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)


# ============================================================
# S3 Paths
# ============================================================

raw_stats_path = f"s3://{S3_BUCKET}/raw/media_stats/load_date={LOAD_DATE}/"
raw_metadata_path = f"s3://{S3_BUCKET}/raw/media_metadata/load_date={LOAD_DATE}/"
raw_engagement_curve_path = f"s3://{S3_BUCKET}/raw/media_engagement_curve/load_date={LOAD_DATE}/"
raw_daily_stats_path = f"s3://{S3_BUCKET}/raw/media_daily_stats/load_date={LOAD_DATE}/"
raw_aggregated_stats_path = f"s3://{S3_BUCKET}/raw/media_aggregated_stats/load_date={LOAD_DATE}/"
raw_inventory_path = f"s3://{S3_BUCKET}/raw/media_inventory/load_date={LOAD_DATE}/"

bronze_media_stats_path = f"s3://{S3_BUCKET}/bronze/media_stats/"
bronze_media_metadata_path = f"s3://{S3_BUCKET}/bronze/media_metadata/"
bronze_engagement_curve_path = f"s3://{S3_BUCKET}/bronze/media_engagement_curve/"
bronze_daily_stats_path = f"s3://{S3_BUCKET}/bronze/media_daily_stats/"
bronze_aggregated_stats_path = f"s3://{S3_BUCKET}/bronze/media_aggregated_stats/"
bronze_inventory_path = f"s3://{S3_BUCKET}/bronze/media_inventory/"

silver_media_stats_path = f"s3://{S3_BUCKET}/silver/media_stats/"
silver_media_metadata_path = f"s3://{S3_BUCKET}/silver/media_metadata/"
silver_engagement_curve_path = f"s3://{S3_BUCKET}/silver/media_engagement_curve/"
silver_daily_stats_path = f"s3://{S3_BUCKET}/silver/media_daily_stats/"
silver_aggregated_stats_path = f"s3://{S3_BUCKET}/silver/media_aggregated_stats/"
silver_inventory_path = f"s3://{S3_BUCKET}/silver/media_inventory/"

gold_fact_media_engagement_path = f"s3://{S3_BUCKET}/gold/fact_media_engagement/"
gold_fact_engagement_curve_path = f"s3://{S3_BUCKET}/gold/fact_engagement_curve/"
gold_fact_media_daily_stats_path = f"s3://{S3_BUCKET}/gold/fact_media_daily_stats/"
gold_dim_media_path = f"s3://{S3_BUCKET}/gold/dim_media/"
gold_dim_media_inventory_path = f"s3://{S3_BUCKET}/gold/dim_media_inventory/"


print(f"Starting transformation for LOAD_DATE={LOAD_DATE}")


# ============================================================
# Helper: Extract media_id from S3 path
# ============================================================

def add_media_id_from_path(df):
    return df.withColumn(
        "media_id",
        regexp_extract(
            input_file_name(),
            r"media_id=([^/]+)",
            1
        )
    )


# ============================================================
# 1. Media Stats: Raw → Bronze → Silver → Gold Fact
# ============================================================

print(f"Reading raw media stats from: {raw_stats_path}")

raw_stats_df = (
    spark.read
    .option("multiline", "true")
    .json(raw_stats_path)
    .drop("media_id")
)

raw_stats_df = add_media_id_from_path(raw_stats_df)

bronze_stats_df = (
    raw_stats_df
    .withColumn("load_date", lit(LOAD_DATE))
    .withColumn("ingestion_timestamp", current_timestamp())
)

print("Writing Bronze media_stats")

bronze_stats_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(bronze_media_stats_path)

silver_stats_df = (
    bronze_stats_df
    .dropDuplicates(["media_id", "load_date"])
    .withColumn("processed_timestamp", current_timestamp())
)

print("Writing Silver media_stats")

silver_stats_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(silver_media_stats_path)

gold_fact_df = (
    silver_stats_df
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

print("Writing Gold fact_media_engagement")

gold_fact_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(gold_fact_media_engagement_path)


# ============================================================
# 2. Media Metadata: Raw → Bronze → Silver → Gold dim_media
# ============================================================

print(f"Reading raw media metadata from: {raw_metadata_path}")

metadata_df = (
    spark.read
    .option("multiline", "true")
    .json(raw_metadata_path)
    .drop("media_id")
)

metadata_df = add_media_id_from_path(metadata_df)

bronze_metadata_df = (
    metadata_df
    .withColumn("load_date", lit(LOAD_DATE))
    .withColumn("ingestion_timestamp", current_timestamp())
)

print("Writing Bronze media_metadata")

bronze_metadata_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(bronze_media_metadata_path)

silver_metadata_df = (
    bronze_metadata_df
    .dropDuplicates(["media_id", "load_date"])
    .withColumn("processed_timestamp", current_timestamp())
)

print("Writing Silver media_metadata")

silver_metadata_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(silver_media_metadata_path)

dim_media_df = (
    silver_metadata_df
    .select(
        col("media_id"),
        col("hashed_id"),
        col("id").alias("wistia_internal_id"),
        col("name").alias("media_title"),
        col("duration"),
        col("created").alias("created_at"),
        col("updated").alias("updated_at"),
        col("status"),
        col("type").alias("media_type"),
        lit(LOAD_DATE).alias("load_date")
    )
    .dropDuplicates(["media_id", "load_date"])
    .withColumn(
        "media_surrogate_key",
        sha2(
            concat_ws(
                "||",
                col("media_id").cast("string"),
                col("hashed_id").cast("string")
            ),
            256
        )
    )
    .withColumn("processed_timestamp", current_timestamp())
)

print("Writing Gold dim_media")

dim_media_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(gold_dim_media_path)


# ============================================================
# 3. Engagement Curve: Raw → Bronze → Silver → Gold Fact
# ============================================================

print(f"Reading raw engagement curve from: {raw_engagement_curve_path}")

engagement_raw_df = (
    spark.read
    .option("multiline", "true")
    .json(raw_engagement_curve_path)
    .drop("media_id")
)

engagement_raw_df = add_media_id_from_path(engagement_raw_df)

bronze_engagement_df = (
    engagement_raw_df
    .withColumn("load_date", lit(LOAD_DATE))
    .withColumn("ingestion_timestamp", current_timestamp())
)

print("Writing Bronze media_engagement_curve")

bronze_engagement_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(bronze_engagement_curve_path)

# Explode engagement_data
engagement_curve_df = (
    bronze_engagement_df
    .select(
        col("media_id"),
        lit(LOAD_DATE).alias("load_date"),
        col("engagement"),
        posexplode(col("engagement_data")).alias("second_index", "engagement_value"),
        col("rewatch_data")
    )
)

# Explode rewatch_data separately
rewatch_curve_df = (
    bronze_engagement_df
    .select(
        col("media_id"),
        lit(LOAD_DATE).alias("load_date"),
        posexplode(col("rewatch_data")).alias("second_index", "rewatch_value")
    )
)

silver_engagement_df = (
    engagement_curve_df
    .join(
        rewatch_curve_df,
        on=["media_id", "load_date", "second_index"],
        how="left"
    )
    .withColumn("processed_timestamp", current_timestamp())
    .dropDuplicates(["media_id", "load_date", "second_index"])
)

print("Writing Silver media_engagement_curve")

silver_engagement_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(silver_engagement_curve_path)

gold_engagement_curve_df = (
    silver_engagement_df
    .withColumn(
        "curve_surrogate_key",
        sha2(
            concat_ws(
                "||",
                col("media_id").cast("string"),
                col("load_date").cast("string"),
                col("second_index").cast("string")
            ),
            256
        )
    )
    .withColumn("load_timestamp", current_timestamp())
)

print("Writing Gold fact_engagement_curve")

gold_engagement_curve_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(gold_fact_engagement_curve_path)


# ============================================================
# 4. Daily Stats: Raw → Bronze → Silver → Gold Fact
# ============================================================

print(f"Reading raw media daily stats from: {raw_daily_stats_path}")

daily_raw_df = (
    spark.read
    .option("multiline", "true")
    .json(raw_daily_stats_path)
    .drop("media_id")
)

daily_raw_df = add_media_id_from_path(daily_raw_df)

bronze_daily_df = (
    daily_raw_df
    .withColumn("load_date", lit(LOAD_DATE))
    .withColumn("ingestion_timestamp", current_timestamp())
)

print("Writing Bronze media_daily_stats")

bronze_daily_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(bronze_daily_stats_path)

silver_daily_df = (
    bronze_daily_df
    .select(
        col("media_id"),
        lit(LOAD_DATE).alias("load_date"),
        explode(col("records")).alias("daily_record")
    )
    .select(
        col("media_id"),
        col("load_date"),
        col("daily_record.date").alias("stat_date"),
        col("daily_record.load_count").alias("load_count"),
        col("daily_record.play_count").alias("play_count"),
        col("daily_record.hours_watched").alias("hours_watched")
    )
    .withColumn("stat_date", to_date(col("stat_date")))
    .withColumn("processed_timestamp", current_timestamp())
    .dropDuplicates(["media_id", "stat_date"])
)

print("Writing Silver media_daily_stats")

silver_daily_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(silver_daily_stats_path)

gold_daily_df = (
    silver_daily_df
    .withColumn(
        "daily_surrogate_key",
        sha2(
            concat_ws(
                "||",
                col("media_id").cast("string"),
                col("stat_date").cast("string")
            ),
            256
        )
    )
    .withColumn("load_timestamp", current_timestamp())
)

print("Writing Gold fact_media_daily_stats")

gold_daily_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(gold_fact_media_daily_stats_path)


# ============================================================
# 5. Aggregated Stats: Raw → Bronze/Silver
# ============================================================

print(f"Reading raw media aggregated stats from: {raw_aggregated_stats_path}")

aggregated_raw_df = (
    spark.read
    .option("multiline", "true")
    .json(raw_aggregated_stats_path)
    .drop("media_id")
)

aggregated_raw_df = add_media_id_from_path(aggregated_raw_df)

bronze_aggregated_df = (
    aggregated_raw_df
    .withColumn("load_date", lit(LOAD_DATE))
    .withColumn("ingestion_timestamp", current_timestamp())
)

print("Writing Bronze media_aggregated_stats")

bronze_aggregated_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(bronze_aggregated_stats_path)

silver_aggregated_df = (
    bronze_aggregated_df
    .select(
        col("media_id"),
        col("id").alias("wistia_internal_id"),
        col("hashed_id"),
        col("name").alias("media_title"),
        col("stats.pageLoads").alias("page_loads"),
        col("stats.visitors").alias("visitors"),
        col("stats.percentOfVisitorsClickingPlay").alias("percent_visitors_clicking_play"),
        col("stats.plays").alias("plays"),
        col("stats.averagePercentWatched").alias("average_percent_watched"),
        lit(LOAD_DATE).alias("load_date")
    )
    .withColumn("processed_timestamp", current_timestamp())
    .dropDuplicates(["media_id", "load_date"])
)

print("Writing Silver media_aggregated_stats")

silver_aggregated_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(silver_aggregated_stats_path)


# ============================================================
# 6. Media Inventory: Raw → Bronze → Silver → Gold Dim
# ============================================================

print(f"Reading raw media inventory from: {raw_inventory_path}")

inventory_raw_df = (
    spark.read
    .option("multiline", "true")
    .json(raw_inventory_path)
)

bronze_inventory_df = (
    inventory_raw_df
    .withColumn("load_date", lit(LOAD_DATE))
    .withColumn("ingestion_timestamp", current_timestamp())
)

print("Writing Bronze media_inventory")

bronze_inventory_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(bronze_inventory_path)

silver_inventory_df = (
    bronze_inventory_df
    .select(
        lit(LOAD_DATE).alias("load_date"),
        explode(col("records")).alias("media")
    )
    .select(
        col("media.id").alias("wistia_internal_id"),
        col("media.hashed_id").alias("media_id"),
        col("media.name").alias("media_title"),
        col("media.duration").alias("duration"),
        col("media.created").alias("created_at"),
        col("media.updated").alias("updated_at"),
        col("media.status").alias("status"),
        col("media.type").alias("media_type"),
        col("media.archived").alias("archived"),
        col("media.folder.name").alias("folder_name"),
        col("media.folder.hashed_id").alias("folder_hashed_id"),
        col("media.section").alias("section"),
        col("media.thumbnail.url").alias("thumbnail_url"),
        col("load_date")
    )
    .withColumn("processed_timestamp", current_timestamp())
    .dropDuplicates(["media_id", "load_date"])
)

print("Writing Silver media_inventory")

silver_inventory_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(silver_inventory_path)

gold_inventory_df = (
    silver_inventory_df
    .withColumn(
        "media_inventory_surrogate_key",
        sha2(
            concat_ws(
                "||",
                col("media_id").cast("string"),
                col("load_date").cast("string")
            ),
            256
        )
    )
    .withColumn("load_timestamp", current_timestamp())
)

print("Writing Gold dim_media_inventory")

gold_inventory_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(gold_dim_media_inventory_path)


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

Amazon S3 – Transformed Data Output

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Wistia-Video-Analytics/main/Phases/Ref%20Files/S3%20after%20transform.png" width="750"/> </p>

