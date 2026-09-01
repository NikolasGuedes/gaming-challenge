import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalstackContainer, StartedLocalStackContainer } from "@testcontainers/localstack";
import { ReceiveMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { MikroOrmOutboxRepository } from "../src/messaging/infrastructure/persistence/repositories/outbox.repository.js";
import { OutboxMessageEntity } from "../src/messaging/infrastructure/persistence/entities/outbox-message.entity.js";
import { bootstrapQueues } from "../src/messaging/infrastructure/sqs/bootstrap-queues.js";
import { OutboxPublisherWorker } from "../src/messaging/infrastructure/sqs/outbox-publisher.worker.js";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env.js";

describe("OutboxPublisherWorker", () => {
  let db: TestDatabase;
  let localstack: StartedLocalStackContainer;
  let sqsClient: SQSClient;
  let queueUrl: string;

  beforeAll(async () => {
    db = await startTestDatabase();
    localstack = await new LocalstackContainer("localstack/localstack:3")
      .withEnvironment({ SERVICES: "sqs" })
      .start();
    sqsClient = new SQSClient({
      region: "us-east-1",
      endpoint: localstack.getConnectionUri(),
      credentials: { accessKeyId: "test", secretAccessKey: "test" },
    });
    ({ mainQueueUrl: queueUrl } = await bootstrapQueues(sqsClient));
    process.env.SQS_QUEUE_URL = queueUrl;
  }, 90_000);

  afterAll(async () => {
    await stopTestDatabase(db);
    await localstack.stop();
  });

  it("publishes a pending outbox row to SQS and marks it published", async () => {
    const setupEm = db.orm.em.fork();
    const outboxRepo = new MikroOrmOutboxRepository(setupEm, db.orm);
    await outboxRepo.append({
      id: "88888888-8888-8888-8888-888888888888",
      aggregateId: "99999999-9999-9999-9999-999999999999",
      eventType: "WagerProcessed",
      payload: { hello: "world" },
      occurredAt: new Date(),
    });
    await setupEm.flush();

    const worker = new OutboxPublisherWorker(sqsClient, db.orm);
    const processed = await worker.tick();
    expect(processed).toBe(1);

    const verifyEm = db.orm.em.fork();
    const row = await verifyEm.findOne(OutboxMessageEntity, { id: "88888888-8888-8888-8888-888888888888" });
    expect(row?.publishedAt).not.toBeNull();

    const received = await sqsClient.send(
      new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 5 }),
    );
    expect(received.Messages).toHaveLength(1);
    const body = JSON.parse(received.Messages![0].Body!);
    expect(body.aggregateId).toBe("99999999-9999-9999-9999-999999999999");
  }, 30_000);
});
