import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalstackContainer, StartedLocalStackContainer } from "@testcontainers/localstack";
import { GetQueueAttributesCommand, SQSClient } from "@aws-sdk/client-sqs";
import { bootstrapQueues } from "../src/messaging/infrastructure/sqs/bootstrap-queues.js";

describe("bootstrapQueues", () => {
  let container: StartedLocalStackContainer;
  let client: SQSClient;

  beforeAll(async () => {
    container = await new LocalstackContainer("localstack/localstack:3")
      .withEnvironment({ SERVICES: "sqs" })
      .start();
    client = new SQSClient({
      region: "us-east-1",
      endpoint: container.getConnectionUri(),
      credentials: { accessKeyId: "test", secretAccessKey: "test" },
    });
  }, 60_000);

  afterAll(async () => {
    await container.stop();
  });

  it("creates the main FIFO queue with a redrive policy pointing at the DLQ", async () => {
    const { mainQueueUrl, dlqUrl, eventsQueueUrl } = await bootstrapQueues(client);
    expect(mainQueueUrl).toContain("wager-transactions.fifo");
    expect(dlqUrl).toContain("wager-transactions-dlq.fifo");
    expect(eventsQueueUrl).toContain("wager-events.fifo");

    const attrs = await client.send(
      new GetQueueAttributesCommand({ QueueUrl: mainQueueUrl, AttributeNames: ["RedrivePolicy"] }),
    );
    const redrivePolicy = JSON.parse(attrs.Attributes!.RedrivePolicy!);
    expect(redrivePolicy.maxReceiveCount).toBe("5");
  });
});
