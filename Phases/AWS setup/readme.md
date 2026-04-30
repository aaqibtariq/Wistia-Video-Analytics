
End-to-end AWS data engineering project using AWS Glue, S3, Delta Lake, Athena, and Streamlit.


# Create S3 Bucket (Data Lake)

- Go to AWS Console → S3
- Click Create bucket
- Configure:
- Bucket name
- wistia-video-analytics-aaqib
- Region
- → Use same region for everything (ex: us-east-1)
- Block Public Access
- Click Create bucket

- Open bucket → click Create folder

- Create these:

```
raw/
bronze/
silver/
gold/
state/
scripts/
athena-results/

```


#  Create Secrets Manager Secret (Wistia API)


- Go to AWS → Secrets Manager
- Click Store a new secret
- Choose:
- Secret type
- → Other type of secret
- Add key-value:
- Key: api_token
- Value: <your_wistia_token>
- Secret name:
- wistia/api_token
- Disable rotation (for now)
- Click Next → Store



# Create Glue IAM Role


- Go to IAM → Roles
- Click Create role
- Select:
- Trusted entity:
  - AWS service
  - Use case:
  - Glue
- Click Next
- Attach Policies
- Add these:
    - AWSGlueServiceRole
    - AmazonS3FullAccess
    - CloudWatchLogsFullAccess
    - SecretsManagerReadWrite
- Role name:
  - GlueExecutionRole-Wistia
- Click Create role
- Make sure Go inside role → Trust relationships → Edit

  ```json
{
  "Effect": "Allow",
  "Principal": {
    "Service": "glue.amazonaws.com"
  },
  "Action": "sts:AssumeRole"
}

```


4. Athena Setup (Query Layer)

- Go to AWS Athena
- Click Query editor
- First-time setup:
- It will ask for result location otherwise click on Query settings -> Manage and 
- Set:
    - s3://wistia-video-analytics-aaqib/athena-results/
-  Click Save
-  Create Database
-  Run:
    -  CREATE DATABASE wistia_analytics;
-  Verify
    -  SHOW DATABASES;

