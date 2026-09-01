import {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { createSqsClient } from "./sqs-client.provider";

export async function bootstrapQueues(
  client: SQSClient,
): Promise<{ mainQueueUrl: string; dlqUrl: string }> {
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

  return { mainQueueUrl: main.QueueUrl!, dlqUrl };
}

if (import.meta.main) {
  bootstrapQueues(createSqsClient())
    .then(({ mainQueueUrl, dlqUrl }) => {
      console.log(`Main queue: ${mainQueueUrl}`);
      console.log(`DLQ: ${dlqUrl}`);
    })
    .catch((err) => {
      console.error(err);
      process.exit(1);
    });
}
