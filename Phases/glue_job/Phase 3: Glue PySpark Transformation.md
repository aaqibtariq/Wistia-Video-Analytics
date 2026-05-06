raw JSON → bronze Delta → silver Delta → gold Delta

## wistia_transform_delta.py

```python

import sys
from datetime import datetime
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

# Dynamic load date generated automatically at runtime
LOAD_DATE = datetime.utcnow().strftime("%Y-%m-%d")

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
    .dropDuplicates()
    .withColumn("processed_timestamp", current_timestamp())
)

print("Writing Silver Delta table")

silver_df.write.format("delta") \
    .mode("append") \
    .partitionBy("load_date") \
    .save(silver_path)

gold_df = (
    silver_df
    .withColumn("engagement_date", to_date(lit(LOAD_DATE)))
    .withColumn(
        "surrogate_key",
        sha2(
            concat_ws(
                "||",
                col("hashed_id").cast("string"),
                col("name").cast("string"),
                lit(LOAD_DATE)
            ),
            256
        )
    )
    .withColumn("load_timestamp", current_timestamp())
)

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




