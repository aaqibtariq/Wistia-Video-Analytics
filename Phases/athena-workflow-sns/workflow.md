
# Create Glue Workflow

```bash

aws glue create-workflow \
  --name wistia_video_analytics_workflow \
  --description "Wistia API ingestion and Delta transformation workflow"
```


# Create Start Trigger

```bash

aws glue create-trigger \
  --name wistia_start_trigger \
  --workflow-name wistia_video_analytics_workflow \
  --type ON_DEMAND \
  --actions JobName=wistia_api_ingestion
```

# Create Transform Trigger

```bash

aws glue create-trigger \
  --name wistia_transform_after_ingestion \
  --workflow-name wistia_video_analytics_workflow \
  --type CONDITIONAL \
  --predicate '{
    "Logical": "AND",
    "Conditions": [
      {
        "LogicalOperator": "EQUALS",
        "JobName": "wistia_api_ingestion",
        "State": "SUCCEEDED"
      }
    ]
  }' \
  --actions JobName=wistia_transform_delta \
  --start-on-creation

```

# Test Workflow

```bash

aws glue start-workflow-run \
  --name wistia_video_analytics_workflow

```

# Check Workflow Run

```bash

aws glue get-workflow-runs \
  --name wistia_video_analytics_workflow \
  --include-graph

```

# Check Job Runs

```bash

aws glue get-job-runs \
  --job-name wistia_api_ingestion \
  --max-results 3


aws glue get-job-runs \
  --job-name wistia_transform_delta \
  --max-results 3

```

# AWS Glue Workflow – Pipeline Setup

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Wistia-Video-Analytics/main/Phases/Ref%20Files/glue%20workflow%20setup%20Wista.png" width="750"/> </p>

# AWS Glue Workflow – Execution Status

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Wistia-Video-Analytics/main/Phases/Ref%20Files/glue%20workflow%20status%20Wista.png" width="750"/> </p>

# AWS Glue Workflow – Monitoring & Runtime Status

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Wistia-Video-Analytics/main/Phases/Ref%20Files/glue%20workflow%20status%20Wistaa.png" width="750"/> </p>
