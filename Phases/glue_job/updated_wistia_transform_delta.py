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
    regexp_extract
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

raw_stats_path = f"s3://{S3_BUCKET}/raw/media_stats/load_date={LOAD_DATE}/"
raw_metadata_path = f"s3://{S3_BUCKET}/raw/media_metadata/load_date={LOAD_DATE}/"

bronze_media_stats_path = f"s3://{S3_BUCKET}/bronze/media_stats/"
silver_media_stats_path = f"s3://{S3_BUCKET}/silver/media_stats/"

gold_fact_media_engagement_path = f"s3://{S3_BUCKET}/gold/fact_media_engagement/"
gold_dim_media_path = f"s3://{S3_BUCKET}/gold/dim_media/"

print(f"Starting transformation for LOAD_DATE={LOAD_DATE}")
print(f"Reading media stats from: {raw_stats_path}")

# ============================================================
# Read Raw Media Stats
# ============================================================

raw_stats_df = (
    spark.read
    .option("multiline", "true")
    .json(raw_stats_path)
    .drop("media_id")
)

raw_stats_df = raw_stats_df.withColumn(
    "media_id",
    regexp_extract(
        input_file_name(),
        r"media_id=([^/]+)",
        1
    )
)

print("===== RAW MEDIA STATS SCHEMA =====")
raw_stats_df.printSchema()

print("===== RAW MEDIA STATS SAMPLE =====")
raw_stats_df.show(5, truncate=False)

# ============================================================
# Bronze: Media Stats
# ============================================================

bronze_stats_df = (
    raw_stats_df
    .withColumn("load_date", lit(LOAD_DATE))
    .withColumn("ingestion_timestamp", current_timestamp())
)

print("Writing Bronze media_stats Delta table")

bronze_stats_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(bronze_media_stats_path)

# ============================================================
# Silver: Media Stats
# ============================================================

silver_stats_df = (
    bronze_stats_df
    .dropDuplicates(["media_id", "load_date"])
    .withColumn("processed_timestamp", current_timestamp())
)

print("Writing Silver media_stats Delta table")

silver_stats_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(silver_media_stats_path)

# ============================================================
# Gold Fact: fact_media_engagement
# ============================================================

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

print("===== GOLD FACT SCHEMA =====")
gold_fact_df.printSchema()

print("===== GOLD FACT SAMPLE =====")
gold_fact_df.show(5, truncate=False)

print("Writing Gold fact_media_engagement Delta table")

gold_fact_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(gold_fact_media_engagement_path)

# ============================================================
# Gold Dimension: dim_media
# ============================================================

print(f"Reading media metadata from: {raw_metadata_path}")

metadata_df = (
    spark.read
    .option("multiline", "true")
    .json(raw_metadata_path)
    .drop("media_id")
)

metadata_df = metadata_df.withColumn(
    "media_id",
    regexp_extract(
        input_file_name(),
        r"media_id=([^/]+)",
        1
    )
)

print("===== RAW MEDIA METADATA SCHEMA =====")
metadata_df.printSchema()

print("===== RAW MEDIA METADATA SAMPLE =====")
metadata_df.show(5, truncate=False)

dim_media_df = (
    metadata_df
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

print("===== GOLD DIM MEDIA SCHEMA =====")
dim_media_df.printSchema()

print("===== GOLD DIM MEDIA SAMPLE =====")
dim_media_df.show(5, truncate=False)

print("Writing Gold dim_media Delta table")

dim_media_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save(gold_dim_media_path)

print("Transformation completed successfully")