import { HttpException, Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import { MikroORM } from "@mikro-orm/postgresql";
import {
  DeleteMessageCommand,
  Message,
  ReceiveMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { Money } from "../../../shared-kernel/money.js";
import { CurrencyMismatchError, InvalidMoneyError } from "../../../shared-kernel/money.errors.js";
import { WagerKind } from "../../../wagering/domain/wager-transaction.js";
import { computePayloadHash } from "../../../wagering/domain/payload-hash.js";
import { ProcessWagerUseCase } from "../../../wagering/application/use-cases/process-wager.use-case.js";
import { MikroOrmWalletRepository } from "../../../wallet/infrastructure/persistence/repositories/wallet.repository.js";
import { MikroOrmWagerTransactionRepository } from "../../../wagering/infrastructure/persistence/repositories/wager-transaction.repository.js";
import { MikroOrmOutboxRepository } from "../persistence/repositories/outbox.repository.js";
import { MikroOrmInboxRepository } from "../persistence/repositories/inbox.repository.js";
import { SQS_CLIENT } from "./sqs-client.provider.js";

const WAGER_TRANSACTION_REQUESTED_TYPE = "WagerTransactionRequested";

/**
 * A failure that says "this message will never succeed" rather than "try again later"
 * (spec §6). Redelivering a malformed amount or a request for a wallet that does not exist
 * burns the full `maxReceiveCount` before the DLQ and gains nothing — those are acked and
 * logged instead. Anything else (a dropped DB connection, an SQS timeout) is genuinely
 * transient and is left unacked so the queue redelivers it.
 */
function isBusinessError(err: unknown): boolean {
  if (err instanceof InvalidMoneyError || err instanceof CurrencyMismatchError || err instanceof SyntaxError) {
    return true;
  }
  // Nest 4xx (NotFoundException for an unknown wallet, BadRequestException, ConflictException…);
  // a 5xx HttpException is treated as transient.
  return err instanceof HttpException && err.getStatus() >= 400 && err.getStatus() < 500;
}

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
    let envelope: WagerTransactionEnvelope;
    try {
      envelope = JSON.parse(message.Body ?? "") as WagerTransactionEnvelope;
    } catch (err) {
      // A body that isn't JSON can never become JSON — poison, not transient.
      await this.skipAck(message, `body is not valid JSON: ${(err as Error).message}`);
      return;
    }

    // Defensive shape check: this queue is also the target of the outbox
    // publisher's `WagerProcessed` domain events (see ARCHITECTURE.md's
    // Known limitations — the two are not meant to share a queue, but the
    // brief only names one). Today those events happen to have an
    // incompatible shape (`eventType`/`payload`, not `type`/`data`), so
    // they'd already fail below and redrive to the DLQ — but only by
    // coincidence, not because anything actually validates the envelope.
    // Reject anything that isn't an actual wager-submission request before
    // touching `envelope.data`, so a future event whose payload happens to
    // resemble a wager submission is never mistaken for one.
    if (envelope?.type !== WAGER_TRANSACTION_REQUESTED_TYPE) {
      await this.skipAck(
        message,
        `unexpected envelope type ${String(envelope?.type)} (not a wager transaction request)`,
      );
      return;
    }

    const em = this.orm.em.fork();

    try {
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
    } catch (err) {
      if (!isBusinessError(err)) {
        throw err; // transient — leave it unacked so SQS redelivers
      }
      // The transaction rolled back, so the inbox row is gone too: nothing was written, and
      // there is nothing to retry. Ack now instead of burning maxReceiveCount redeliveries.
      await this.skipAck(message, `business rule rejected the message: ${(err as Error).message}`);
      return;
    }

    await this.ack(message);
  }

  /** Deletes a message the consumer refuses to process, so the queue stops redelivering it. */
  private async skipAck(message: Message, reason: string): Promise<void> {
    this.logger.warn(`message ${message.MessageId} skipped — ${reason}`);
    await this.ack(message);
  }

  private async ack(message: Message): Promise<void> {
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
