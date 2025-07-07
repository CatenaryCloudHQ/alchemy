import { describe, expect } from "vitest";
import { alchemy } from "../../src/alchemy.js";
import { Job } from "../../src/databricks/job.js";
import { destroy } from "../../src/destroy.js";
import "../../src/test/vitest.js";
import { BRANCH_PREFIX } from "../util.js";

const test = alchemy.test(import.meta, {
  prefix: BRANCH_PREFIX,
});

describe("Databricks Job", () => {
  test("create, update, and delete notebook job", async (scope) => {
    const jobId = `${BRANCH_PREFIX}-test-job`;
    let job: Job;

    try {
      // CREATE
      job = await Job(jobId, {
        name: "Test Notebook Job",
        tasks: [
          {
            task_key: "test_task",
            notebook_task: {
              notebook_path: "/Shared/test_notebook",
              base_parameters: {
                env: "test",
              },
            },
          },
        ],
      });

      expect(job).toMatchObject({
        name: "Test Notebook Job",
        tasks: [
          {
            task_key: "test_task",
            notebook_task: {
              notebook_path: "/Shared/test_notebook",
              base_parameters: {
                env: "test",
              },
            },
          },
        ],
      });
      expect(job.id).toBeDefined();
      expect(job.created_time).toBeDefined();

      // UPDATE
      job = await Job(jobId, {
        name: "Updated Test Job",
        tasks: [
          {
            task_key: "test_task",
            notebook_task: {
              notebook_path: "/Shared/test_notebook",
              base_parameters: {
                env: "test",
                updated: "true",
              },
            },
          },
        ],
      });

      expect(job).toMatchObject({
        name: "Updated Test Job",
        tasks: [
          {
            task_key: "test_task",
            notebook_task: {
              notebook_path: "/Shared/test_notebook",
              base_parameters: {
                env: "test",
                updated: "true",
              },
            },
          },
        ],
      });
    } finally {
      await destroy(scope);
      await assertJobDoesNotExist(job!);
    }
  });

  test("create job with schedule and notifications", async (scope) => {
    const jobId = `${BRANCH_PREFIX}-scheduled-job`;
    let job: Job;

    try {
      job = await Job(jobId, {
        name: "Scheduled Test Job",
        tasks: [
          {
            task_key: "scheduled_task",
            notebook_task: {
              notebook_path: "/Shared/scheduled_notebook",
            },
          },
        ],
        schedule: {
          quartz_cron_expression: "0 0 * * * ?",
          timezone_id: "UTC",
        },
        email_notifications: {
          on_failure: ["test@example.com"],
        },
        max_concurrent_runs: 1,
      });

      expect(job).toMatchObject({
        name: "Scheduled Test Job",
        schedule: {
          quartz_cron_expression: "0 0 * * * ?",
          timezone_id: "UTC",
        },
        email_notifications: {
          on_failure: ["test@example.com"],
        },
        max_concurrent_runs: 1,
      });
    } finally {
      await destroy(scope);
      await assertJobDoesNotExist(job!);
    }
  });
});

async function assertJobDoesNotExist(job: Job) {
  if (!job?.id) return;

  const baseUrl = process.env.DATABRICKS_HOST;
  const token = process.env.DATABRICKS_TOKEN;

  if (!baseUrl || !token) {
    throw new Error("Missing required environment variables");
  }

  const response = await fetch(`${baseUrl}/api/2.1/jobs/get?job_id=${job.id}`, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
  });

  if (response.ok) {
    throw new Error(`Job ${job.id} still exists after deletion`);
  }
}
