import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalstackContainer, StartedLocalStackContainer } from "@testcontainers/localstack";
import { ReceiveMessageCommand, SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { Money } from "../src/shared-kernel/money.js";
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
});
