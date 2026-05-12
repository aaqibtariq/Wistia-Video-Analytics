
# Create SNS topic

```bash

aws sns create-topic \
  --name wistia-pipeline-alerts

```

# Subscribe your email

```bash

aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:wistia-pipeline-alerts \
  --protocol email \
  --notification-endpoint your-email@example.com

```


# Create failure alert rule

```bash

aws events put-rule \
  --name wistia-glue-failure-alert \
  --event-pattern '{
    "source": ["aws.glue"],
    "detail-type": ["Glue Job State Change"],
    "detail": {
      "jobName": ["wistia_api_ingestion", "wistia_transform_delta"],
      "state": ["FAILED", "TIMEOUT", "STOPPED"]
    }
  }'

Add SNS as target:


aws events put-targets \
  --rule wistia-glue-failure-alert \
  --targets "Id"="1","Arn"="arn:aws:sns:us-east-1:ACCOUNT_ID:wistia-pipeline-alerts"

```

# Create success alert rule

```bash

aws events put-rule \
  --name wistia-glue-success-alert \
  --event-pattern '{
    "source": ["aws.glue"],
    "detail-type": ["Glue Job State Change"],
    "detail": {
      "jobName": ["wistia_transform_delta"],
      "state": ["SUCCEEDED"]
    }
  }'

Add SNS as target:

aws events put-targets \
  --rule wistia-glue-success-alert \
  --targets "Id"="1","Arn"="arn:aws:sns:us-east-1:ACCOUNT_ID:wistia-pipeline-alerts"


```

# Test

```bash

aws glue start-workflow-run \
  --name wistia_video_analytics_workflow

aws events list-targets-by-rule \
  --rule wistia-glue-success-alert

aws events list-targets-by-rule \
  --rule wistia-glue-failure-alert

```


# Amazon SNS – Workflow Notifications

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Wistia-Video-Analytics/main/Phases/Ref%20Files/SNS%20for%20Wistia.png" width="750"/> </p>
