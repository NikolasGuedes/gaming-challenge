import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { SQSClient } from "@aws-sdk/client-sqs";
import { HealthController } from "../src/health/health.controller";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env";

describe("HealthController", () => {
  let db: TestDatabase;

  beforeAll(async () => {
    db = await startTestDatabase();
  }, 60_000);

  afterAll(async () => {
    await stopTestDatabase(db);
  });

  it("GET /health/live always returns ok", () => {
    const controller = new HealthController(db.orm.em.fork(), new SQSClient({ region: "us-east-1" }));
    expect(controller.live()).toEqual({ status: "ok" });
  });

  it("GET /health/ready returns ok when Postgres and SQS are both reachable", async () => {
    const sqsClient = new SQSClient({
      region: "us-east-1",
      endpoint: process.env.AWS_ENDPOINT_URL ?? "http://localhost:4566",
      credentials: { accessKeyId: "test", secretAccessKey: "test" },
    });
    const controller = new HealthController(db.orm.em.fork(), sqsClient);
    await expect(controller.ready()).resolves.toEqual({ database: "ok", sqs: "ok" });
  });

  it("GET /health/ready reports sqs: error and throws 503 when SQS is unreachable", async () => {
    const unreachableSqsClient = new SQSClient({
      region: "us-east-1",
      endpoint: "http://localhost:1", // nothing listens here
      credentials: { accessKeyId: "test", secretAccessKey: "test" },
      maxAttempts: 1,
    });
    const controller = new HealthController(db.orm.em.fork(), unreachableSqsClient);
    await expect(controller.ready()).rejects.toThrow();
  });
});
