import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import { MikroORM } from "@mikro-orm/postgresql";
import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { MikroOrmOutboxRepository } from "../persistence/repositories/outbox.repository.js";
import { SQS_CLIENT } from "./sqs-client.provider.js";

const POLL_INTERVAL_MS = 2000;
const BATCH_SIZE = 10;

@Injectable()
export class OutboxPublisherWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxPublisherWorker.name);
  private readonly queueUrl = process.env.SQS_EVENTS_QUEUE_URL ?? "";
  private timer: ReturnType<typeof setInterval> | null = null;
  private ticking = false;

  constructor(
    @Inject(SQS_CLIENT) private readonly sqsClient: SQSClient,
    private readonly orm: MikroORM,
  ) {}

  onModuleInit(): void {
    if (!this.queueUrl) {
      this.logger.warn("SQS_EVENTS_QUEUE_URL not set — outbox publisher disabled");
      return;
    }
    this.timer = setInterval(() => {
      void this.tick();
    }, POLL_INTERVAL_MS);
  }

  onModuleDestroy(): void {
    if (this.timer) {
      clearInterval(this.timer);
    }
  }

  async tick(): Promise<number> {
    if (this.ticking) {
      return 0;
    }
    this.ticking = true;
    try {
      const repository = new MikroOrmOutboxRepository(this.orm.em.fork(), this.orm);
      return await repository.processDueBatch(BATCH_SIZE, async (message) => {
        try {
          await this.sqsClient.send(
            new SendMessageCommand({
              QueueUrl: this.queueUrl,
              MessageBody: JSON.stringify(
                typeof message.payload === "object" && message.payload !== null && "eventType" in message.payload
                  ? message.payload
                  : {
                      eventType: message.eventType,
                      aggregateId: message.aggregateId,
                      occurredAt: message.occurredAt,
                      payload: message.payload,
                    },
              ),
              MessageGroupId: message.aggregateId,
              MessageDeduplicationId: message.id,
            }),
          );
          return true;
        } catch (err) {
          this.logger.error(`failed to publish outbox message ${message.id}`, err as Error);
          return false;
        }
      });
    } finally {
      this.ticking = false;
    }
  }
}
