import type { Context } from "../context.js";
import { Resource } from "../resource.js";

/**
 * Task settings for a Databricks job
 */
export interface TaskSettings {
  /**
   * The task key that uniquely identifies the task
   */
  task_key: string;

  /**
   * The language that the task runs
   */
  language?: "python" | "scala" | "sql";

  /**
   * The source code that the task executes
   */
  source?: string;

  /**
   * Notebook task settings
   */
  notebook_task?: {
    /**
     * The path of the notebook to run
     */
    notebook_path: string;

    /**
     * Base parameters for the notebook
     */
    base_parameters?: Record<string, string>;
  };

  /**
   * Spark Python task settings
   */
  spark_python_task?: {
    /**
     * Python file to be executed
     */
    python_file: string;

    /**
     * Command line parameters passed to the Python file
     */
    parameters?: string[];
  };

  /**
   * Spark JAR task settings
   */
  spark_jar_task?: {
    /**
     * JAR file to be executed
     */
    main_class_name: string;

    /**
     * Parameters passed to the main class
     */
    parameters?: string[];
  };

  /**
   * SQL task settings
   */
  sql_task?: {
    /**
     * Query to be executed
     */
    query: string;

    /**
     * Warehouse ID where the query should run
     */
    warehouse_id?: string;
  };
}

/**
 * Properties for creating/updating a Databricks Job
 */
export interface JobProps {
  /**
   * A name for the job
   */
  name: string;

  /**
   * Tasks that the job will execute
   */
  tasks: TaskSettings[];

  /**
   * Optional schedule for automated job runs
   */
  schedule?: {
    /**
     * Cron schedule expression
     */
    quartz_cron_expression: string;

    /**
     * Timezone for the schedule (e.g. "UTC")
     */
    timezone_id: string;

    /**
     * Whether the schedule is paused
     */
    pause_status?: "PAUSED" | "UNPAUSED";
  };

  /**
   * Maximum number of concurrent runs
   */
  max_concurrent_runs?: number;

  /**
   * Email notifications configuration
   */
  email_notifications?: {
    /**
     * List of emails to notify on start
     */
    on_start?: string[];

    /**
     * List of emails to notify on success
     */
    on_success?: string[];

    /**
     * List of emails to notify on failure
     */
    on_failure?: string[];
  };

  /**
   * Optional timeout in seconds
   */
  timeout_seconds?: number;
}

/**
 * Represents a Databricks Job resource
 */
export interface Job extends Resource<"databricks::job">, JobProps {
  /**
   * The unique identifier of the job
   */
  id: string;

  /**
   * The timestamp when the job was created
   */
  created_time?: number;
}

/**
 * Creates and manages a Databricks Job
 *
 * @example
 * ## Basic Python Notebook Job
 *
 * Creates a job that runs a Python notebook on a schedule
 *
 * ```typescript
 * const job = await Job("data-processing", {
 *   name: "Daily Data Processing",
 *   tasks: [{
 *     task_key: "process_data",
 *     notebook_task: {
 *       notebook_path: "/Shared/process_data",
 *       base_parameters: {
 *         data_date: "2024-01-01"
 *       }
 *     }
 *   }],
 *   schedule: {
 *     quartz_cron_expression: "0 0 1 * * ?",
 *     timezone_id: "UTC"
 *   },
 *   email_notifications: {
 *     on_failure: ["team@example.com"]
 *   }
 * });
 * ```
 *
 * @example
 * ## Multi-Task Job
 *
 * Creates a job with multiple dependent tasks
 *
 * ```typescript
 * const job = await Job("etl-pipeline", {
 *   name: "ETL Pipeline",
 *   tasks: [
 *     {
 *       task_key: "extract",
 *       notebook_task: {
 *         notebook_path: "/ETL/extract"
 *       }
 *     },
 *     {
 *       task_key: "transform",
 *       notebook_task: {
 *         notebook_path: "/ETL/transform"
 *       }
 *     },
 *     {
 *       task_key: "load",
 *       notebook_task: {
 *         notebook_path: "/ETL/load"
 *       }
 *     }
 *   ],
 *   max_concurrent_runs: 1
 * });
 * ```
 */
export const Job = Resource(
  "databricks::job",
  async function (
    this: Context<Job, JobProps>,
    id: string,
    props: JobProps,
  ): Promise<Job> {
    const baseUrl = process.env.DATABRICKS_HOST;
    const token = process.env.DATABRICKS_TOKEN;

    if (!baseUrl) {
      throw new Error("DATABRICKS_HOST environment variable is required");
    }
    if (!token) {
      throw new Error("DATABRICKS_TOKEN environment variable is required");
    }

    const headers = {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    };

    if (this.phase === "delete") {
      if (this.output?.id) {
        const response = await fetch(`${baseUrl}/api/2.1/jobs/delete`, {
          method: "POST",
          headers,
          body: JSON.stringify({ job_id: this.output.id }),
        });

        // Don't throw on 404s during deletion
        if (!response.ok && response.status !== 404) {
          throw new Error(`Failed to delete job: ${response.statusText}`);
        }
      }
      return this.destroy();
    }

    const jobSettings = {
      name: props.name,
      tasks: props.tasks,
      schedule: props.schedule,
      max_concurrent_runs: props.max_concurrent_runs,
      email_notifications: props.email_notifications,
      timeout_seconds: props.timeout_seconds,
    };

    if (this.phase === "update" && this.output?.id) {
      // Update existing job
      const response = await fetch(`${baseUrl}/api/2.1/jobs/update`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          job_id: this.output.id,
          new_settings: jobSettings,
        }),
      });

      if (!response.ok) {
        throw new Error(`Failed to update job: ${response.statusText}`);
      }

      return this({
        ...props,
        id: this.output.id,
        created_time: this.output.created_time,
      });
    } else {
      // Create new job
      const response = await fetch(`${baseUrl}/api/2.1/jobs/create`, {
        method: "POST",
        headers,
        body: JSON.stringify(jobSettings),
      });

      if (!response.ok) {
        throw new Error(`Failed to create job: ${response.statusText}`);
      }

      const data = (await response.json()) as { job_id: number };
      return this({
        ...props,
        id: data.job_id.toString(),
        created_time: Date.now(),
      });
    }
  },
);
