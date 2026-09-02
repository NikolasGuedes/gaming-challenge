import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { Money } from "../src/shared-kernel/money.js";
import { FailureCode } from "../src/shared-kernel/failure-code.js";
import { CreateWalletUseCase } from "../src/wallet/application/use-cases/create-wallet.use-case.js";
import { MikroOrmWalletRepository } from "../src/wallet/infrastructure/persistence/repositories/wallet.repository.js";
import { ProcessWagerUseCase } from "../src/wagering/application/use-cases/process-wager.use-case.js";
import { MikroOrmWagerTransactionRepository } from "../src/wagering/infrastructure/persistence/repositories/wager-transaction.repository.js";
import { MikroOrmOutboxRepository } from "../src/messaging/infrastructure/persistence/repositories/outbox.repository.js";
import { PendingReferenceWorker } from "../src/messaging/infrastructure/sqs/pending-reference.worker.js";
import { OutboxMessageEntity } from "../src/messaging/infrastructure/persistence/entities/outbox-message.entity.js";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env.js";

describe("PendingReferenceWorker", () => {
  let db: TestDatabase;

  beforeAll(async () => { db = await startTestDatabase(); }, 60_000);
  afterAll(async () => { await stopTestDatabase(db); });

  it("rejects an expired missing reference and emits WagerTransactionRejected", async () => {
    const createEm = db.orm.em.fork({ useContext: true });
    const wallet = await new CreateWalletUseCase(
      createEm,
      new MikroOrmWalletRepository(createEm),
      new MikroOrmWagerTransactionRepository(createEm),
    ).execute({
      playerId: "player-pending-worker",
      currency: "BRL",
      initialBalance: Money.from({ amount: "100.00", currency: "BRL" }),
    });

    const processEm = db.orm.em.fork({ useContext: true });
    const result = await new ProcessWagerUseCase(
      processEm,
      new MikroOrmWalletRepository(processEm),
      new MikroOrmWagerTransactionRepository(processEm),
      new MikroOrmOutboxRepository(processEm, db.orm),
    ).execute({
      playerId: "player-pending-worker",
      roundId: "round-pending-worker",
      gameId: "game-pending-worker",
      externalTransactionId: "refund-missing-worker",
      providerId: "provider-worker",
      idempotencyKey: "idem-refund-missing-worker",
      payloadHash: "hash-refund-missing-worker",
      kind: "REFUND",
      walletId: wallet.id,
      amount: Money.from({ amount: "10.00", currency: "BRL" }),
      referenceExternalTransactionId: "never-arrives",
    });
    expect(result.status).toBe("PENDING_REFERENCE");

    await db.orm.em.getConnection().execute(
      `update wager_transactions
       set reference_attempts = 4, next_reference_attempt_at = now() - interval '1 second'
       where id = ?`,
      [result.transactionId],
    );

    expect(await new PendingReferenceWorker(db.orm).tick(new Date())).toBe(1);
    const tx = await new MikroOrmWagerTransactionRepository(db.orm.em.fork()).findById(result.transactionId);
    expect(tx?.status).toBe("REJECTED");
    expect(tx?.failureCode).toBe(FailureCode.REFERENCE_NOT_FOUND);
    const rejectedEvent = await db.orm.em.fork().findOne(OutboxMessageEntity, {
      aggregateId: wallet.id,
      eventType: "WagerTransactionRejected",
    });
    expect(rejectedEvent).not.toBeNull();
  });
});
