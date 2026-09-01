import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import { MikroORM } from "@mikro-orm/postgresql";
import {
  DeleteMessageCommand,
  Message,
  ReceiveMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { Money } from "../../../shared-kernel/money.js";
import { WagerKind } from "../../../wagering/domain/wager-transaction.js";
import { computePayloadHash } from "../../../wagering/domain/payload-hash.js";
import { ProcessWagerUseCase } from "../../../wagering/application/use-cases/process-wager.use-case.js";
import { MikroOrmWalletRepository } from "../../../wallet/infrastructure/persistence/repositories/wallet.repository.js";
import { MikroOrmWagerTransactionRepository } from "../../../wagering/infrastructure/persistence/repositories/wager-transaction.repository.js";
import { MikroOrmOutboxRepository } from "../persistence/repositories/outbox.repository.js";
import { MikroOrmInboxRepository } from "../persistence/repositories/inbox.repository.js";
import { SQS_CLIENT } from "./sqs-client.provider.js";

export interface WagerTransactionEnvelope {
  messageId: string;
  type: string;
  occurredAt: string;
  data: {
    providerId: string;
    externalTransactionId: string;
    idempotencyKey: string;
    playerId: string;
    walletId: string;
    roundId?: string;
    gameId?: string;
    kind: WagerKind;
    money: { amount: string; currency: string };
    referenceExternalTransactionId?: string;
  };
}

@Injectable()
export class WagerTransactionConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(WagerTransactionConsumer.name);
  private readonly consumerName = "wager-transaction-consumer";
  private readonly queueUrl = process.env.SQS_QUEUE_URL ?? "";
  private stopped = false;
  private inFlight = 0;
  private pollingPromise: Promise<void> | null = null;

  constructor(
    @Inject(SQS_CLIENT) private readonly sqsClient: SQSClient,
    private readonly orm: MikroORM,
  ) {}

  onModuleInit(): void {
    if (!this.queueUrl) {
      this.logger.warn("SQS_QUEUE_URL not set — consumer will not start polling");
      return;
    }
    this.pollingPromise = this.pollLoop();
  }

  async onModuleDestroy(): Promise<void> {
    this.stopped = true;
    await this.pollingPromise;
    while (this.inFlight > 0) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  }

  private async pollLoop(): Promise<void> {
    while (!this.stopped) {
      const result = await this.sqsClient.send(
        new ReceiveMessageCommand({ QueueUrl: this.queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 5 }),
      );
      for (const message of result.Messages ?? []) {
        if (this.stopped) break;
        this.inFlight += 1;
        try {
          await this.handleMessage(message);
        } catch (err) {
          this.logger.error(`failed to process message ${message.MessageId}`, err as Error);
        } finally {
          this.inFlight -= 1;
        }
      }
    }
  }

  async handleMessage(message: Message): Promise<void> {
    const envelope = JSON.parse(message.Body!) as WagerTransactionEnvelope;
    const em = this.orm.em.fork();

    await em.transactional(async (tx) => {
      const inboxRepository = new MikroOrmInboxRepository(tx);
      const alreadyProcessed = await inboxRepository.exists(this.consumerName, message.MessageId!);
      if (alreadyProcessed) {
        this.logger.debug(`message ${message.MessageId} already processed — skipping (dedup)`);
        return;
      }
      await inboxRepository.markProcessed(this.consumerName, message.MessageId!);

      const useCase = new ProcessWagerUseCase(
        tx,
        new MikroOrmWalletRepository(tx),
        new MikroOrmWagerTransactionRepository(tx),
        new MikroOrmOutboxRepository(tx, this.orm),
      );
      await useCase.execute({
        externalTransactionId: envelope.data.externalTransactionId,
        providerId: envelope.data.providerId,
        idempotencyKey: envelope.data.idempotencyKey,
        payloadHash: computePayloadHash(envelope.data as unknown as Record<string, unknown>),
        kind: envelope.data.kind,
        walletId: envelope.data.walletId,
        amount: Money.from(envelope.data.money),
        referenceExternalTransactionId: envelope.data.referenceExternalTransactionId ?? null,
      });
    });

    try {
      await this.sqsClient.send(
        new DeleteMessageCommand({ QueueUrl: this.queueUrl, ReceiptHandle: message.ReceiptHandle! }),
      );
    } catch (err) {
      // A redundant delete (already removed, or a stale receipt handle after redelivery)
      // is not a failure — the message is gone either way.
      this.logger.debug(`ack for message ${message.MessageId} skipped: ${(err as Error).message}`);
    }
  }
}
