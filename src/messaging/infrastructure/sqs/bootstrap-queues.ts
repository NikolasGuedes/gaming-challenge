import {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { createSqsClient } from "./sqs-client.provider.js";

export async function bootstrapQueues(
  client: SQSClient,
): Promise<{ mainQueueUrl: string; dlqUrl: string; eventsQueueUrl: string }> {
  const dlq = await client.send(
    new CreateQueueCommand({
      QueueName: "wager-transactions-dlq.fifo",
      Attributes: { FifoQueue: "true", ContentBasedDeduplication: "false" },
    }),
  );
  const dlqUrl = dlq.QueueUrl!;

  const dlqAttrs = await client.send(
    new GetQueueAttributesCommand({ QueueUrl: dlqUrl, AttributeNames: ["QueueArn"] }),
  );
  const dlqArn = dlqAttrs.Attributes!.QueueArn!;

  const main = await client.send(
    new CreateQueueCommand({
      QueueName: "wager-transactions.fifo",
      Attributes: {
        FifoQueue: "true",
        ContentBasedDeduplication: "false",
        VisibilityTimeout: "30",
        RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlqArn, maxReceiveCount: "5" }),
      },
    }),
  );

  const events = await client.send(
    new CreateQueueCommand({
      QueueName: "wager-events.fifo",
      Attributes: { FifoQueue: "true", ContentBasedDeduplication: "false" },
    }),
  );

  return { mainQueueUrl: main.QueueUrl!, dlqUrl, eventsQueueUrl: events.QueueUrl! };
}

if (import.meta.main) {
  bootstrapQueues(createSqsClient())
    .then(({ mainQueueUrl, dlqUrl, eventsQueueUrl }) => {
      console.log(`Main queue: ${mainQueueUrl}`);
      console.log(`DLQ: ${dlqUrl}`);
      console.log(`Events queue: ${eventsQueueUrl}`);
    })
    .catch((err) => {
      console.error(err);
      process.exit(1);
    });
}
