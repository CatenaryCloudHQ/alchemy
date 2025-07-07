# Databricks Provider

The Databricks provider allows you to manage resources in your Databricks workspace using Infrastructure as Code.

## Authentication

The provider requires the following environment variables to be set:

- `DATABRICKS_HOST` - The URL of your Databricks workspace (e.g., https://your-workspace.cloud.databricks.com)
- `DATABRICKS_TOKEN` - A [personal access token](https://docs.databricks.com/dev-tools/auth.html#personal-access-tokens) with appropriate permissions

## Resources

- [Job](./job.md) - Manages Databricks Jobs for scheduling and running tasks

## Example Usage

```typescript
import { Job } from "alchemy/databricks";

// Create a scheduled notebook job
const job = await Job("etl-job", {
  name: "Daily ETL Process",
  tasks: [{
    task_key: "process_data",
    notebook_task: {
      notebook_path: "/ETL/process_data",
      base_parameters: {
        date: "2024-01-01"
      }
    }
  }],
  schedule: {
    quartz_cron_expression: "0 0 1 * * ?", // Run daily at 1 AM
    timezone_id: "UTC"
  },
  email_notifications: {
    on_failure: ["team@example.com"]
  }
});