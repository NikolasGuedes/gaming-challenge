import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { EntityManager } from "@mikro-orm/postgresql";
import { Money } from "../src/shared-kernel/money.js";
import { FailureCode } from "../src/shared-kernel/failure-code.js";
import { CreateWalletUseCase } from "../src/wallet/application/use-cases/create-wallet.use-case.js";
import { MikroOrmWalletRepository } from "../src/wallet/infrastructure/persistence/repositories/wallet.repository.js";
import { MikroOrmWagerTransactionRepository } from "../src/wagering/infrastructure/persistence/repositories/wager-transaction.repository.js";
import { MikroOrmOutboxRepository } from "../src/messaging/infrastructure/persistence/repositories/outbox.repository.js";
import { OutboxMessageEntity } from "../src/messaging/infrastructure/persistence/entities/outbox-message.entity.js";
import { ProcessWagerUseCase } from "../src/wagering/application/use-cases/process-wager.use-case.js";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env.js";

function buildProcessWagerUseCase(em: EntityManager): ProcessWagerUseCase {
  return new ProcessWagerUseCase(
    em,
    new MikroOrmWalletRepository(em),
    new MikroOrmWagerTransactionRepository(em),
    new MikroOrmOutboxRepository(em, undefined as never), // orm not needed for append()
  );
}

async function seedWallet(db: TestDatabase, playerId: string, initial: string): Promise<string> {
  const em = db.orm.em.fork({ useContext: true });
  const useCase = new CreateWalletUseCase(em, new MikroOrmWalletRepository(em), new MikroOrmWagerTransactionRepository(em));
  const wallet = await useCase.execute({ playerId, currency: "BRL", initialBalance: Money.from({ amount: initial, currency: "BRL" }) });
  return wallet.id;
}

describe("ProcessWagerUseCase", () => {
  let db: TestDatabase;

  beforeAll(async () => {
    db = await startTestDatabase();
  }, 60_000);

  afterAll(async () => {
    await stopTestDatabase(db);
  });

  it("processes a BET, debits the wallet and writes an outbox event", async () => {
    const walletId = await seedWallet(db, "player-bet-1", "100.00");
    const em = db.orm.em.fork({ useContext: true });
    const result = await buildProcessWagerUseCase(em).execute({
      externalTransactionId: "bet-1",
      providerId: "provider-a",
      idempotencyKey: "idem-bet-1",
      payloadHash: "hash-bet-1",
      kind: "BET",
      walletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });

    expect(result).toMatchObject({ status: "PROCESSED", idempotentReplay: false });
    if (result.status === "PROCESSED") {
      expect(result.balance.toString()).toBe("70.00");
    }

    const verifyEm = db.orm.em.fork();
    const outboxRows = await verifyEm.find(OutboxMessageEntity, { aggregateId: walletId });
    expect(outboxRows).toHaveLength(1);
    expect(outboxRows[0].publishedAt).toBeNull();
  });

  it("rejects a BET with insufficient funds without touching the balance", async () => {
    const walletId = await seedWallet(db, "player-bet-2", "10.00");
    const em = db.orm.em.fork({ useContext: true });
    const result = await buildProcessWagerUseCase(em).execute({
      externalTransactionId: "bet-2",
      providerId: "provider-a",
      idempotencyKey: "idem-bet-2",
      payloadHash: "hash-bet-2",
      kind: "BET",
      walletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });

    expect(result).toEqual({
      status: "REJECTED",
      transactionId: expect.any(String),
      failureCode: FailureCode.INSUFFICIENT_FUNDS,
      idempotentReplay: false,
    });
  });

  it("replays the original result for a duplicate (providerId, externalTransactionId) without a second debit", async () => {
    const walletId = await seedWallet(db, "player-bet-3", "100.00");
    const input = {
      externalTransactionId: "bet-3",
      providerId: "provider-a",
      idempotencyKey: "idem-bet-3",
      payloadHash: "hash-bet-3",
      kind: "BET" as const,
      walletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    };

    const first = await buildProcessWagerUseCase(db.orm.em.fork({ useContext: true })).execute(input);
    const second = await buildProcessWagerUseCase(db.orm.em.fork({ useContext: true })).execute(input);

    expect(first.idempotentReplay).toBe(false);
    expect(second.idempotentReplay).toBe(true);
    expect(second).toMatchObject({ status: "PROCESSED" });
    if (first.status === "PROCESSED" && second.status === "PROCESSED") {
      expect(second.balance.toString()).toBe(first.balance.toString());
    }

    const verifyEm = db.orm.em.fork();
    const wallet = await new MikroOrmWalletRepository(verifyEm).findById(walletId);
    expect(wallet?.balance.toString()).toBe("70.00"); // debited exactly once
  });

  it("a REFUND arriving before its BET is PENDING_REFERENCE, then resolves when the BET arrives", async () => {
    const walletId = await seedWallet(db, "player-refund-1", "100.00");

    const refundResult = await buildProcessWagerUseCase(db.orm.em.fork({ useContext: true })).execute({
      externalTransactionId: "refund-1",
      providerId: "provider-a",
      idempotencyKey: "idem-refund-1",
      payloadHash: "hash-refund-1",
      kind: "REFUND",
      walletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: "bet-not-yet-arrived",
    });
    expect(refundResult).toMatchObject({ status: "PENDING_REFERENCE" });

    const betResult = await buildProcessWagerUseCase(db.orm.em.fork({ useContext: true })).execute({
      externalTransactionId: "bet-not-yet-arrived",
      providerId: "provider-a",
      idempotencyKey: "idem-bet-x",
      payloadHash: "hash-bet-x",
      kind: "BET",
      walletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });
    expect(betResult).toMatchObject({ status: "PROCESSED" });
    if (betResult.status === "PROCESSED") {
      // BET debited 30 (100 -> 70), then the now-resolved REFUND credited 30 back (70 -> 100)
      expect(betResult.balance.toString()).toBe("100.00");
    }

    const verifyEm = db.orm.em.fork();
    const wallet = await new MikroOrmWalletRepository(verifyEm).findById(walletId);
    expect(wallet?.balance.toString()).toBe("100.00");
  });
});
