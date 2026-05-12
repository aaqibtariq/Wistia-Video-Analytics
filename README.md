# Wistia-Video-Analytics



## Objective

Build a scalable cloud-based video analytics platform that collects, processes, transforms, and visualizes Wistia engagement data using AWS services. 
The platform enables businesses to analyze viewer behavior, video engagement trends, and rewatch patterns through automated data pipelines and interactive dashboards.

## Core Objective

The core objective of this project is to create an end-to-end data engineering solution that:

- Extracts video analytics data from the Wistia API
- Stores raw and processed data in Amazon S3
- Performs transformations using AWS Glue PySpark
- Automates workflows using Glue Workflow
- Queries analytical datasets using Amazon Athena
- Sends operational alerts using SNS
- Visualizes business insights through Streamlit dashboards hosted on EC2

## Project Goal

The goal of this project is to help organizations understand how users interact with video content by analyzing:

- Viewer engagement
- Rewatch behavior
- Average watch duration
- Video retention trends
- Top-performing content
- Audience interaction patterns

This project demonstrates how modern AWS data engineering services can be integrated to build a fully automated analytics platform capable of handling scalable video engagement reporting.


# Abstract


Modern businesses heavily rely on video content for marketing, education, onboarding, and customer engagement. However, raw engagement data from video platforms often remains underutilized due to the lack of centralized analytics pipelines.

This project solves that problem by building a complete AWS-based analytics ecosystem for Wistia video data. The pipeline extracts engagement metrics from the Wistia API, processes the data using AWS Glue PySpark jobs, stores datasets in Amazon S3, and enables serverless SQL analysis through Athena.

The system is orchestrated using AWS Glue Workflow, while operational monitoring is handled using SNS notifications and CloudWatch logging. Finally, Streamlit dashboards hosted on EC2 provide interactive visualizations for business users to monitor viewer engagement and video performance.

The architecture follows modern cloud-native and serverless data engineering practices designed for scalability, automation, monitoring, and business intelligence reporting.


## Architecture Components

- Wistia API
    - Source system for video analytics data
- AWS Glue
    - Data ingestion and transformation
- AWS Glue PySpark
    - Data cleansing and enrichment
- Amazon S3	Data lake
    - storage layer
- AWS Glue Workflow	Pipeline
    - orchestration
- Amazon Athena	Serverless
    - SQL querying
- Amazon SNS
    - Workflow monitoring and alerts
- Amazon CloudWatch
    - Logging and monitoring
- EC2
    - Hosts Streamlit dashboard
- Streamlit
    - Business analytics dashboard
- Python
    - ETL scripting language
- SQL
    - Analytical querying
- Plotly
    - Interactive visualizations

# Key Metrics

The platform calculates and visualizes several important video engagement metrics including:

- Metric
    - Description
- Average Engagement
    - Measures average viewer interaction with videos
- Rewatch Value
    - Identifies repeated viewing behavior
- Curve Points
    - Tracks engagement across video timeline
- Watch Duration
    - Measures total viewing time
- Viewer Retention
    - Analyzes drop-off behavior
- Top Videos
    - Identifies highest-performing videos
- Daily Engagement Trends
    - Monitors engagement over time
- Media Performance
    - Compares content performance
- Audience Interaction
    - Evaluates user viewing patterns

# Project Phases

- Phase 1	AWS Infrastructure Setup
    - [AWS Setup & Configuration](https://github.com/aaqibtariq/Wistia-Video-Analytics/blob/main/Phases/AWS%20setup/readme.md)
      
- Phase 2	Wistia API Data Ingestion using AWS Glue
    - [Phase 2: Glue Ingestion Job](https://github.com/aaqibtariq/Wistia-Video-Analytics/blob/main/Phases/glue_job/Phase%202%3A%20Glue%20Ingestion%20Job.md)
      
- Phase 3	PySpark Data Transformation
    - [Phase 3: Glue PySpark Transformation](https://github.com/aaqibtariq/Wistia-Video-Analytics/blob/main/Phases/glue_job/Phase%203%3A%20Glue%20PySpark%20Transformation.md)
      
- Phase 4	Amazon Athena External Table Setup
    - [Athena Queries & Analytics](https://github.com/aaqibtariq/Wistia-Video-Analytics/blob/main/Phases/athena-workflow-sns/Athena.md)
      
- Phase 5	AWS Glue Workflow Orchestration
    - [Glue Workflow Configuration](https://github.com/aaqibtariq/Wistia-Video-Analytics/blob/main/Phases/athena-workflow-sns/workflow.md)
      
- Phase 6	SNS Monitoring and Alerts
    - [SNS Alerts & Notifications](https://github.com/aaqibtariq/Wistia-Video-Analytics/blob/main/Phases/athena-workflow-sns/sns.md)
      
- Phase 7	Streamlit Dashboard Deployment on EC2
    - [EC2 Setup & Configuration](https://github.com/aaqibtariq/Wistia-Video-Analytics/blob/main/Phases/AWS%20setup/EC2.md)
    - [Streamlit Application](https://github.com/aaqibtariq/Wistia-Video-Analytics/tree/main/Phases/streamlit)
      
- Phase 8	Business Analytics Visualization
    - [Dashboard Visualizations](https://github.com/aaqibtariq/Wistia-Video-Analytics/tree/main/Phases/Dashboards)
