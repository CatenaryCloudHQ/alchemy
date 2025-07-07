# Databricks Job

The Job resource allows you to manage Databricks Jobs, which can run notebooks, Python scripts, JAR files, or SQL queries on a schedule or on-demand.

## Example Usage

### Basic Notebook Job

```typescript
import { Job } from "alchemy/databricks";

const job = await Job("data-processing", {
  name: "Daily Data Processing",
  tasks: [{
    task_key: "process_data",
    notebook_task: {
      notebook_path: "/Shared/process_data",
      base_parameters: {
        data_date: "2024-01-01"
      }
    }
  }],
  schedule: {
    quartz_cron_expression: "0 0 1 * * ?",
    timezone_id: "UTC"
  },
  email_notifications: {
    on_failure: ["team@example.com"]
  }
});
```

### Multi-Task Pipeline

```typescript
const job = await Job("etl-pipeline", {
  name: "ETL Pipeline",
  tasks: [
    {
      task_key: "extract",
      notebook_task: {
        notebook_path: "/ETL/extract"
      }
    },
    {
      task_key: "transform",
      notebook_task: {
        notebook_path: "/ETL/transform"
      }
    },
    {
      task_key: "load",
      notebook_task: {
        notebook_path: "/ETL/load"
      }
    }
  ],
  max_concurrent_runs: 1
});
```

### Python Script Job

```typescript
const job = await Job("python-job", {
  name: "Python Script Job",
  tasks: [{
    task_key: "run_script",
    spark_python_task: {
      python_file: "dbfs:/scripts/process.py",
      parameters: ["--date", "2024-01-01"]
    }
  }]
});
```

### SQL Job

```typescript
const job = await Job("sql-job", {
  name: "SQL Analysis Job",
  tasks: [{
    task_key: "run_analysis",
    sql_task: {
      query: "SELECT * FROM my_table WHERE date = '2024-01-01'",
      warehouse_id: "my_warehouse_id"
    }
  }]
});
```

## Arguments

### Required Arguments

- `name` (string) - A name for the job
- `tasks` (TaskSettings[]) - Array of tasks that the job will execute

### Optional Arguments

- `schedule` (object) - Schedule configuration for automated job runs
  - `quartz_cron_expression` (string) - Cron schedule expression
  - `timezone_id` (string) - Timezone for the schedule
  - `pause_status` ("PAUSED" | "UNPAUSED") - Whether the schedule is paused
- `max_concurrent_runs` (number) - Maximum number of concurrent job runs
- `email_notifications` (object) - Email notification settings
  - `on_start` (string[]) - Emails to notify when job starts
  - `on_success` (string[]) - Emails to notify when job succeeds
  - `on_failure` (string[]) - Emails to notify when job fails
- `timeout_seconds` (number) - Job timeout in seconds

## Task Types

### Notebook Task

```typescript
{
  task_key: "notebook_task",
  notebook_task: {
    notebook_path: string,
    base_parameters?: Record<string, string>
  }
}
```

### Python Task

```typescript
{
  task_key: "python_task",
  spark_python_task: {
    python_file: string,
    parameters?: string[]
  }
}
```

### JAR Task

```typescript
{
  task_key: "jar_task",
  spark_jar_task: {
    main_class_name: string,
    parameters?: string[]
  }
}
```

### SQL Task

```typescript
{
  task_key: "sql_task",
  sql_task: {
    query: string,
    warehouse_id?: string
  }
}
```

## Attributes

- `id` (string) - The unique identifier of the job
- `created_time` (number) - Timestamp when the job was created