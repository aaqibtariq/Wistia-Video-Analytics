
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


