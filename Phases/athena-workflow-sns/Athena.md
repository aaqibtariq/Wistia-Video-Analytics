
# tables in Athena
```sql


DROP TABLE IF EXISTS wistia_analytics.bronze_media_stats;

CREATE EXTERNAL TABLE wistia_analytics.bronze_media_stats
LOCATION 's3://wistia-video-analytics-at/bronze/media_stats/'
TBLPROPERTIES ('table_type'='DELTA');


DROP TABLE IF EXISTS wistia_analytics.silver_media_stats;

CREATE EXTERNAL TABLE wistia_analytics.silver_media_stats
LOCATION 's3://wistia-video-analytics-at/silver/media_stats/'
TBLPROPERTIES ('table_type'='DELTA');


DROP TABLE IF EXISTS wistia_analytics.gold_fact_media_engagement;

CREATE EXTERNAL TABLE wistia_analytics.gold_fact_media_engagement
LOCATION 's3://wistia-video-analytics-at/gold/fact_media_engagement/'
TBLPROPERTIES ('table_type'='DELTA');

DROP TABLE IF EXISTS wistia_analytics.dim_media;

CREATE EXTERNAL TABLE wistia_analytics.dim_media
LOCATION 's3://wistia-video-analytics-at/gold/dim_media/'
TBLPROPERTIES ('table_type'='DELTA');


DROP TABLE IF EXISTS wistia_analytics.gold_fact_engagement_curve;

CREATE EXTERNAL TABLE wistia_analytics.gold_fact_engagement_curve
LOCATION 's3://wistia-video-analytics-at/gold/fact_engagement_curve/'
TBLPROPERTIES ('table_type'='DELTA');


DROP TABLE IF EXISTS wistia_analytics.gold_fact_media_daily_stats;

CREATE EXTERNAL TABLE wistia_analytics.gold_fact_media_daily_stats
LOCATION 's3://wistia-video-analytics-at/gold/fact_media_daily_stats/'
TBLPROPERTIES ('table_type'='DELTA');



DROP TABLE IF EXISTS wistia_analytics.gold_dim_media_inventory;

CREATE EXTERNAL TABLE wistia_analytics.gold_dim_media_inventory
LOCATION 's3://wistia-video-analytics-at/gold/dim_media_inventory/'
TBLPROPERTIES ('table_type'='DELTA');

```


# Verification


```sql
SHOW TABLES IN wistia_analytics;

SELECT COUNT(*) FROM wistia_analytics.bronze_media_stats;
SELECT COUNT(*) FROM wistia_analytics.silver_media_stats;
SELECT COUNT(*) FROM wistia_analytics.gold_fact_media_engagement;
SELECT COUNT(*) FROM wistia_analytics.dim_media;
SELECT COUNT(*) FROM wistia_analytics.gold_fact_engagement_curve;
SELECT COUNT(*) FROM wistia_analytics.gold_fact_media_daily_stats;
SELECT COUNT(*) FROM wistia_analytics.gold_dim_media_inventory;


```


# Amazon Athena – Wistia Analytics Tables

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Wistia-Video-Analytics/main/Phases/Ref%20Files/atehana%20tables%20for%20Wistia.png" width="750"/> </p>
