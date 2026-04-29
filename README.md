# Wistia-Video-Analytics


End-to-end AWS data engineering project using AWS Glue, S3, Delta Lake, Athena, and Streamlit.

## Architecture

Wistia API → AWS Glue Ingestion Job → S3 Raw → AWS Glue PySpark Transform → S3 Delta Silver/Gold → Glue Data Catalog → Athena → Streamlit

## Key Features

- Wistia API ingestion
- Pagination handling
- Incremental loading using checkpoint.json
- Bronze/Silver/Gold medallion architecture
- Delta format for Silver and Gold
- Deduplication using surrogate keys
- Athena query layer
- Streamlit dashboard
- GitHub Actions CI/CD
