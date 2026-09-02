import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalstackContainer, StartedLocalStackContainer } from "@testcontainers/localstack";
import { ReceiveMessageCommand, SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { Money } from "../src/shared-kernel/money.js";
import { FailureCode } from "../src/shared-kernel/failure-code.js";
import { CreateWalletUseCase } from "../src/wallet/application/use-cases/create-wallet.use-case.js";
import { MikroOrmWalletRepository } from "../src/wallet/infrastructure/persistence/repositories/wallet.repository.js";
import { MikroOrmWagerTransactionRepository } from "../src/wagering/infrastructure/persistence/repositories/wager-transaction.repository.js";
import { bootstrapQueues } from "../src/messaging/infrastructure/sqs/bootstrap-queues.js";
import { WagerTransactionConsumer } from "../src/messaging/infrastructure/sqs/wager-transaction.consumer.js";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env.js";

describe("WagerTransactionConsumer — inbox dedup on redelivery", () => {
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
    const queues = await bootstrapQueues(sqsClient);
    queueUrl = queues.mainQueueUrl;
  }, 90_000);

  afterAll(async () => {
    await stopTestDatabase(db);
    await localstack.stop();
  });

  it("processes a redelivered message exactly once", async () => {
    const em = db.orm.em.fork({ useContext: true });
    const wallet = await new CreateWalletUseCase(
      em,
      new MikroOrmWalletRepository(em),
      new MikroOrmWagerTransactionRepository(em),
    ).execute({
      playerId: "player-inbox-1",
      currency: "BRL",
      initialBalance: Money.from({ amount: "100.00", currency: "BRL" }),
    });

    const envelope = {
      messageId: "envelope-1",
      type: "WagerTransactionRequested",
      occurredAt: new Date().toISOString(),
      data: {
        providerId: "provider-sqs",
        externalTransactionId: "sqs-bet-1",
        idempotencyKey: "idem-sqs-1",
        playerId: "player-inbox-1",
        walletId: wallet.id,
        kind: "BET",
        money: { amount: "40.00", currency: "BRL" },
      },
    };

    await sqsClient.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify(envelope),
        MessageGroupId: wallet.id,
        MessageDeduplicationId: "dedup-1",
      }),
    );
    const received = await sqsClient.send(
      new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 5 }),
    );
    const message = received.Messages![0];

    const consumer = new WagerTransactionConsumer(sqsClient, db.orm);
    // Simulate the queue redelivering the same message before the first ack lands.
    await consumer.handleMessage(message);
    await consumer.handleMessage(message);

    const verifyEm = db.orm.em.fork();
    const finalWallet = await new MikroOrmWalletRepository(verifyEm).findById(wallet.id);
    expect(finalWallet?.balance.toString()).toBe("60.00"); // debited exactly once, not twice
  }, 30_000);

  it("rejects an OPENING submitted over SQS instead of crediting the wallet", async () => {
    const em = db.orm.em.fork({ useContext: true });
    const wallet = await new CreateWalletUseCase(
      em,
      new MikroOrmWalletRepository(em),
      new MikroOrmWagerTransactionRepository(em),
    ).execute({
      playerId: "player-inbox-opening",
      currency: "BRL",
      initialBalance: Money.from({ amount: "100.00", currency: "BRL" }),
    });

    const envelope = {
      messageId: "envelope-opening",
      type: "WagerTransactionRequested",
      occurredAt: new Date().toISOString(),
      data: {
        providerId: "provider-sqs",
        externalTransactionId: "sqs-opening-1",
        idempotencyKey: "idem-sqs-opening-1",
        playerId: "player-inbox-opening",
        walletId: wallet.id,
        kind: "OPENING",
        money: { amount: "1000000.00", currency: "BRL" },
      },
    };

    await sqsClient.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify(envelope),
        MessageGroupId: wallet.id,
        MessageDeduplicationId: "dedup-opening-1",
      }),
    );
    const received = await sqsClient.send(
      new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 10 }),
    );
    await new WagerTransactionConsumer(sqsClient, db.orm).handleMessage(received.Messages![0]);

    const verifyEm = db.orm.em.fork();
    const recorded = await new MikroOrmWagerTransactionRepository(verifyEm).findByProviderAndExternalId(
      "provider-sqs",
      "sqs-opening-1",
    );
    expect(recorded?.status).toBe("REJECTED");
    expect(recorded?.failureCode).toBe(FailureCode.INVALID_KIND);

    const finalWallet = await new MikroOrmWalletRepository(verifyEm).findById(wallet.id);
    expect(finalWallet?.balance.toString()).toBe("100.00"); // untouched — no phantom credit
  }, 30_000);

  it("acks a business-invalid message immediately instead of leaving it for redelivery", async () => {
    const envelope = {
      messageId: "envelope-malformed",
      type: "WagerTransactionRequested",
      occurredAt: new Date().toISOString(),
      data: {
        providerId: "provider-sqs",
        externalTransactionId: "sqs-malformed-1",
        idempotencyKey: "idem-sqs-malformed-1",
        playerId: "player-malformed",
        walletId: "00000000-0000-0000-0000-000000000000",
        kind: "BET",
        money: { amount: "not-a-number", currency: "BRL" }, // Money.from throws InvalidMoneyError
      },
    };

    await sqsClient.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify(envelope),
        MessageGroupId: "group-malformed",
        MessageDeduplicationId: "dedup-malformed-1",
      }),
    );
    const received = await sqsClient.send(
      new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 10 }),
    );
    const message = received.Messages![0];

    // Must resolve, not throw: throwing is what leaves the message unacked for redelivery.
    await new WagerTransactionConsumer(sqsClient, db.orm).handleMessage(message);

    const redelivered = await sqsClient.send(
      new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 5 }),
    );
    expect(redelivered.Messages ?? []).toHaveLength(0); // acked — never redriven
  }, 30_000);

  it("acks a foreign envelope type (the self-published WagerProcessed event) without processing it", async () => {
    await sqsClient.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify({
          id: "outbox-1",
          eventType: "WagerProcessed",
          payload: { walletId: "w", balance: { amount: "1.00", currency: "BRL" } },
        }),
        MessageGroupId: "group-foreign",
        MessageDeduplicationId: "dedup-foreign-1",
      }),
    );
    const received = await sqsClient.send(
      new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 10 }),
    );

    await new WagerTransactionConsumer(sqsClient, db.orm).handleMessage(received.Messages![0]);

    const redelivered = await sqsClient.send(
      new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 5 }),
    );
    expect(redelivered.Messages ?? []).toHaveLength(0);
  }, 30_000);
});
