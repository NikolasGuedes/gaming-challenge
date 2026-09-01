import { Module } from "@nestjs/common";
import { SQS_CLIENT, createSqsClient } from "./infrastructure/sqs/sqs-client.provider.js";
import { OUTBOX_REPOSITORY } from "./application/ports/outbox.repository.js";
import { INBOX_REPOSITORY } from "./application/ports/inbox.repository.js";
import { MikroOrmOutboxRepository } from "./infrastructure/persistence/repositories/outbox.repository.js";
import { MikroOrmInboxRepository } from "./infrastructure/persistence/repositories/inbox.repository.js";
import { WagerTransactionConsumer } from "./infrastructure/sqs/wager-transaction.consumer.js";
import { OutboxPublisherWorker } from "./infrastructure/sqs/outbox-publisher.worker.js";

@Module({
  providers: [
    { provide: SQS_CLIENT, useFactory: createSqsClient },
    { provide: OUTBOX_REPOSITORY, useClass: MikroOrmOutboxRepository },
    { provide: INBOX_REPOSITORY, useClass: MikroOrmInboxRepository },
    WagerTransactionConsumer,
    OutboxPublisherWorker,
  ],
  exports: [OUTBOX_REPOSITORY, SQS_CLIENT],
})
export class MessagingModule {}
