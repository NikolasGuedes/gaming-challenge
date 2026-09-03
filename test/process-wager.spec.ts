import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { EntityManager } from "@mikro-orm/postgresql";
import { ConflictException } from "@nestjs/common";
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
    expect(outboxRows).toHaveLength(2);
    expect(outboxRows.map((row) => row.eventType).sort()).toEqual([
      "WagerTransactionProcessed",
      "WalletBalanceChanged",
    ]);
    expect(outboxRows.every((row) => row.publishedAt === null)).toBe(true);
  });

  it("processes LOSS without changing balance, version or ledger", async () => {
    const walletId = await seedWallet(db, "player-loss-1", "100.00");
    const beforeRepo = new MikroOrmWalletRepository(db.orm.em.fork());
    const before = await beforeRepo.findById(walletId);
    const beforeLedger = await beforeRepo.listLedgerEntries(walletId, { limit: 20 });

    const result = await buildProcessWagerUseCase(db.orm.em.fork({ useContext: true })).execute({
      externalTransactionId: "loss-1",
      providerId: "provider-a",
      idempotencyKey: "idem-loss-1",
      payloadHash: "hash-loss-1",
      playerId: "player-loss-1",
      roundId: "round-loss-1",
      gameId: "game-loss-1",
      kind: "LOSS",
      walletId,
      amount: Money.from({ amount: "25.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });

    expect(result).toMatchObject({ status: "PROCESSED" });
    const verifyRepo = new MikroOrmWalletRepository(db.orm.em.fork());
    const after = await verifyRepo.findById(walletId);
    const afterLedger = await verifyRepo.listLedgerEntries(walletId, { limit: 20 });
    expect(after?.balance.toString()).toBe("100.00");
    expect(after?.version).toBe(before?.version);
    expect(afterLedger).toHaveLength(beforeLedger.length);

    const outbox = await db.orm.em.fork().find(OutboxMessageEntity, { aggregateId: walletId });
    expect(outbox.map((row) => row.eventType)).toEqual(["WagerTransactionProcessed"]);
  });

  it("rejects a reversal whose submitted amount differs from its reference", async () => {
    const walletId = await seedWallet(db, "player-refund-amount", "100.00");
    const common = { playerId: "player-refund-amount", roundId: "round-amount", gameId: "game-amount" };
    await buildProcessWagerUseCase(db.orm.em.fork({ useContext: true })).execute({
      ...common,
      externalTransactionId: "bet-refund-amount",
      providerId: "provider-a",
      idempotencyKey: "idem-bet-refund-amount",
      payloadHash: "hash-bet-refund-amount",
      kind: "BET",
      walletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });
    const refund = await buildProcessWagerUseCase(db.orm.em.fork({ useContext: true })).execute({
      ...common,
      externalTransactionId: "refund-wrong-amount",
      providerId: "provider-a",
      idempotencyKey: "idem-refund-wrong-amount",
      payloadHash: "hash-refund-wrong-amount",
      kind: "REFUND",
      walletId,
      amount: Money.from({ amount: "1.00", currency: "BRL" }),
      referenceExternalTransactionId: "bet-refund-amount",
    });
    expect(refund).toMatchObject({ status: "REJECTED", failureCode: FailureCode.REVERSAL_AMOUNT_MISMATCH });
    expect((await new MikroOrmWalletRepository(db.orm.em.fork()).findById(walletId))?.balance.toString()).toBe("70.00");
  });

  it("serializes two concurrent REFUNDs and credits the reference exactly once", async () => {
    const walletId = await seedWallet(db, "player-refund-concurrent", "100.00");
    const common = { playerId: "player-refund-concurrent", roundId: "round-concurrent", gameId: "game-concurrent" };
    await buildProcessWagerUseCase(db.orm.em.fork({ useContext: true })).execute({
      ...common,
      externalTransactionId: "bet-concurrent-refund",
      providerId: "provider-a",
      idempotencyKey: "idem-bet-concurrent-refund",
      payloadHash: "hash-bet-concurrent-refund",
      kind: "BET",
      walletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });
    const refundInput = (suffix: string) => ({
      ...common,
      externalTransactionId: `refund-concurrent-${suffix}`,
      providerId: "provider-a",
      idempotencyKey: `idem-refund-concurrent-${suffix}`,
      payloadHash: `hash-refund-concurrent-${suffix}`,
      kind: "REFUND" as const,
      walletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: "bet-concurrent-refund",
    });
    const results = await Promise.all([
      buildProcessWagerUseCase(db.orm.em.fork({ useContext: true })).execute(refundInput("a")),
      buildProcessWagerUseCase(db.orm.em.fork({ useContext: true })).execute(refundInput("b")),
    ]);
    expect(results.map((result) => result.status).sort()).toEqual(["PROCESSED", "REJECTED"]);
    expect((await new MikroOrmWalletRepository(db.orm.em.fork()).findById(walletId))?.balance.toString()).toBe("100.00");
    const ledger = await new MikroOrmWalletRepository(db.orm.em.fork()).listLedgerEntries(walletId, { limit: 20 });
    expect(ledger.filter((entry) => entry.direction === "CREDIT")).toHaveLength(2); // OPENING + one REFUND
  });

  it("rejects a reversal from a different round", async () => {
    const walletId = await seedWallet(db, "player-round-mismatch", "100.00");
    const useCase = () => buildProcessWagerUseCase(db.orm.em.fork({ useContext: true }));
    await useCase().execute({
      playerId: "player-round-mismatch",
      roundId: "round-original",
      gameId: "game-a",
      externalTransactionId: "bet-round-mismatch",
      providerId: "provider-a",
      idempotencyKey: "idem-bet-round-mismatch",
      payloadHash: "hash-bet-round-mismatch",
      kind: "BET",
      walletId,
      amount: Money.from({ amount: "20.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });
    const result = await useCase().execute({
      playerId: "player-round-mismatch",
      roundId: "round-other",
      gameId: "game-a",
      externalTransactionId: "refund-round-mismatch",
      providerId: "provider-a",
      idempotencyKey: "idem-refund-round-mismatch",
      payloadHash: "hash-refund-round-mismatch",
      kind: "REFUND",
      walletId,
      amount: Money.from({ amount: "20.00", currency: "BRL" }),
      referenceExternalTransactionId: "bet-round-mismatch",
    });
    expect(result).toMatchObject({ status: "REJECTED", failureCode: FailureCode.ROUND_MISMATCH });
  });

  it("uses a distinct failure code when rolling back a credit would make the balance negative", async () => {
    const walletId = await seedWallet(db, "player-negative-rollback", "0.00");
    const common = { playerId: "player-negative-rollback", roundId: "round-negative", gameId: "game-a" };
    const useCase = () => buildProcessWagerUseCase(db.orm.em.fork({ useContext: true }));
    await useCase().execute({
      ...common,
      externalTransactionId: "win-negative-rollback",
      providerId: "provider-a",
      idempotencyKey: "idem-win-negative-rollback",
      payloadHash: "hash-win-negative-rollback",
      kind: "WIN",
      walletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });
    await useCase().execute({
      ...common,
      externalTransactionId: "bet-spends-win",
      providerId: "provider-a",
      idempotencyKey: "idem-bet-spends-win",
      payloadHash: "hash-bet-spends-win",
      kind: "BET",
      walletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });
    const rollback = await useCase().execute({
      ...common,
      externalTransactionId: "rollback-negative",
      providerId: "provider-a",
      idempotencyKey: "idem-rollback-negative",
      payloadHash: "hash-rollback-negative",
      kind: "ROLLBACK",
      walletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: "win-negative-rollback",
    });
    expect(rollback).toMatchObject({
      status: "REJECTED",
      failureCode: FailureCode.REVERSAL_WOULD_MAKE_BALANCE_NEGATIVE,
    });
    expect((await new MikroOrmWalletRepository(db.orm.em.fork()).findById(walletId))?.balance.toString()).toBe("0.00");
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

  it("rejects an idempotency key reused with a different payload in the shared use case", async () => {
    const walletId = await seedWallet(db, "player-idempotency-conflict", "100.00");
    const useCase = () => buildProcessWagerUseCase(db.orm.em.fork({ useContext: true }));
    await useCase().execute({
      externalTransactionId: "bet-idem-original",
      providerId: "provider-a",
      idempotencyKey: "idem-shared-conflict",
      payloadHash: "hash-original",
      kind: "BET",
      walletId,
      amount: Money.from({ amount: "25.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });

    try {
      await useCase().execute({
        externalTransactionId: "bet-idem-conflicting-payload",
        providerId: "provider-a",
        idempotencyKey: "idem-shared-conflict",
        payloadHash: "hash-different",
        kind: "BET",
        walletId,
        amount: Money.from({ amount: "50.00", currency: "BRL" }),
        referenceExternalTransactionId: null,
      });
      throw new Error("expected idempotency conflict");
    } catch (error) {
      expect(error).toBeInstanceOf(ConflictException);
      expect((error as ConflictException).getResponse()).toMatchObject({
        failureCode: FailureCode.IDEMPOTENCY_KEY_CONFLICT,
      });
    }

    expect((await new MikroOrmWalletRepository(db.orm.em.fork()).findById(walletId))?.balance.toString()).toBe("75.00");
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

  it("rejects a second REFUND referencing an already-reversed BET with REFERENCE_ALREADY_REVERSED", async () => {
    const walletId = await seedWallet(db, "player-refund-2", "100.00");

    const betResult = await buildProcessWagerUseCase(db.orm.em.fork({ useContext: true })).execute({
      externalTransactionId: "bet-double-refund",
      providerId: "provider-a",
      idempotencyKey: "idem-bet-double-refund",
      payloadHash: "hash-bet-double-refund",
      kind: "BET",
      walletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });
    expect(betResult).toMatchObject({ status: "PROCESSED" });

    const firstRefund = await buildProcessWagerUseCase(db.orm.em.fork({ useContext: true })).execute({
      externalTransactionId: "refund-first",
      providerId: "provider-a",
      idempotencyKey: "idem-refund-first",
      payloadHash: "hash-refund-first",
      kind: "REFUND",
      walletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: "bet-double-refund",
    });
    expect(firstRefund).toMatchObject({ status: "PROCESSED" });

    const secondRefund = await buildProcessWagerUseCase(db.orm.em.fork({ useContext: true })).execute({
      externalTransactionId: "refund-second",
      providerId: "provider-a",
      idempotencyKey: "idem-refund-second",
      payloadHash: "hash-refund-second",
      kind: "REFUND",
      walletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: "bet-double-refund",
    });
    expect(secondRefund).toEqual({
      status: "REJECTED",
      transactionId: expect.any(String),
      failureCode: FailureCode.REFERENCE_ALREADY_REVERSED,
      idempotentReplay: false,
    });

    const verifyEm = db.orm.em.fork();
    const wallet = await new MikroOrmWalletRepository(verifyEm).findById(walletId);
    // BET debited 30 (100 -> 70), only the first REFUND credited 30 back (70 -> 100); the
    // second REFUND must not have touched the balance a second time.
    expect(wallet?.balance.toString()).toBe("100.00");
  });

  it("credits only the FIRST of two pending REFUNDs when the referenced BET finally arrives", async () => {
    const walletId = await seedWallet(db, "player-refund-3", "100.00");

    for (const suffix of ["a", "b"]) {
      const pending = await buildProcessWagerUseCase(db.orm.em.fork({ useContext: true })).execute({
        externalTransactionId: `refund-race-${suffix}`,
        providerId: "provider-a",
        idempotencyKey: `idem-refund-race-${suffix}`,
        payloadHash: `hash-refund-race-${suffix}`,
        kind: "REFUND",
        walletId,
        amount: Money.from({ amount: "30.00", currency: "BRL" }),
        referenceExternalTransactionId: "bet-race",
      });
      expect(pending).toMatchObject({ status: "PENDING_REFERENCE" });
    }

    const betResult = await buildProcessWagerUseCase(db.orm.em.fork({ useContext: true })).execute({
      externalTransactionId: "bet-race",
      providerId: "provider-a",
      idempotencyKey: "idem-bet-race",
      payloadHash: "hash-bet-race",
      kind: "BET",
      walletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });
    expect(betResult).toMatchObject({ status: "PROCESSED" });

    const verifyEm = db.orm.em.fork();
    const repo = new MikroOrmWagerTransactionRepository(verifyEm);
    const first = await repo.findByProviderAndExternalId("provider-a", "refund-race-a");
    const second = await repo.findByProviderAndExternalId("provider-a", "refund-race-b");
    const statuses = [first!.status, second!.status].sort();
    expect(statuses).toEqual(["PROCESSED", "REJECTED"]);
    const rejected = first!.status === "REJECTED" ? first! : second!;
    expect(rejected.failureCode).toBe(FailureCode.REFERENCE_ALREADY_REVERSED);

    // BET debited 30 (100 -> 70) and exactly ONE refund credited 30 back (70 -> 100).
    const wallet = await new MikroOrmWalletRepository(verifyEm).findById(walletId);
    expect(wallet?.balance.toString()).toBe("100.00");
    if (betResult.status === "PROCESSED") {
      expect(betResult.balance.toString()).toBe("100.00");
    }
  });

  it("rejects a REFUND whose walletId is not the referenced BET's wallet, leaving both wallets untouched", async () => {
    const ownerWalletId = await seedWallet(db, "player-refund-owner", "100.00");
    const attackerWalletId = await seedWallet(db, "player-refund-attacker", "50.00");

    const betResult = await buildProcessWagerUseCase(db.orm.em.fork({ useContext: true })).execute({
      externalTransactionId: "bet-cross-wallet",
      providerId: "provider-a",
      idempotencyKey: "idem-bet-cross-wallet",
      payloadHash: "hash-bet-cross-wallet",
      kind: "BET",
      walletId: ownerWalletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });
    expect(betResult).toMatchObject({ status: "PROCESSED" });

    const refund = await buildProcessWagerUseCase(db.orm.em.fork({ useContext: true })).execute({
      externalTransactionId: "refund-cross-wallet",
      providerId: "provider-a",
      idempotencyKey: "idem-refund-cross-wallet",
      payloadHash: "hash-refund-cross-wallet",
      kind: "REFUND",
      walletId: attackerWalletId, // NOT the wallet the BET belongs to
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: "bet-cross-wallet",
    });
    expect(refund).toEqual({
      status: "REJECTED",
      transactionId: expect.any(String),
      failureCode: FailureCode.WALLET_MISMATCH,
      idempotentReplay: false,
    });

    const verifyEm = db.orm.em.fork();
    const walletRepository = new MikroOrmWalletRepository(verifyEm);
    expect((await walletRepository.findById(ownerWalletId))?.balance.toString()).toBe("70.00");
    expect((await walletRepository.findById(attackerWalletId))?.balance.toString()).toBe("50.00");
  });

  it("rejects a pending REFUND resolved against a BET on a different wallet with WALLET_MISMATCH", async () => {
    const ownerWalletId = await seedWallet(db, "player-pending-owner", "100.00");
    const attackerWalletId = await seedWallet(db, "player-pending-attacker", "50.00");

    const pending = await buildProcessWagerUseCase(db.orm.em.fork({ useContext: true })).execute({
      externalTransactionId: "refund-pending-cross",
      providerId: "provider-a",
      idempotencyKey: "idem-refund-pending-cross",
      payloadHash: "hash-refund-pending-cross",
      kind: "REFUND",
      walletId: attackerWalletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: "bet-pending-cross",
    });
    expect(pending).toMatchObject({ status: "PENDING_REFERENCE" });

    const betResult = await buildProcessWagerUseCase(db.orm.em.fork({ useContext: true })).execute({
      externalTransactionId: "bet-pending-cross",
      providerId: "provider-a",
      idempotencyKey: "idem-bet-pending-cross",
      payloadHash: "hash-bet-pending-cross",
      kind: "BET",
      walletId: ownerWalletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });
    expect(betResult).toMatchObject({ status: "PROCESSED" });

    const verifyEm = db.orm.em.fork();
    const resolved = await new MikroOrmWagerTransactionRepository(verifyEm).findByProviderAndExternalId(
      "provider-a",
      "refund-pending-cross",
    );
    expect(resolved?.status).toBe("REJECTED");
    expect(resolved?.failureCode).toBe(FailureCode.WALLET_MISMATCH);

    const walletRepository = new MikroOrmWalletRepository(verifyEm);
    expect((await walletRepository.findById(ownerWalletId))?.balance.toString()).toBe("70.00");
    expect((await walletRepository.findById(attackerWalletId))?.balance.toString()).toBe("50.00");
  });

  it("rejects an externally submitted OPENING with INVALID_KIND without crediting the wallet", async () => {
    const walletId = await seedWallet(db, "player-opening-1", "100.00");

    const result = await buildProcessWagerUseCase(db.orm.em.fork({ useContext: true })).execute({
      externalTransactionId: "opening-injected",
      providerId: "provider-a",
      idempotencyKey: "idem-opening-injected",
      payloadHash: "hash-opening-injected",
      kind: "OPENING",
      walletId,
      amount: Money.from({ amount: "1000.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });

    expect(result).toEqual({
      status: "REJECTED",
      transactionId: expect.any(String),
      failureCode: FailureCode.INVALID_KIND,
      idempotentReplay: false,
    });

    const verifyEm = db.orm.em.fork();
    const wallet = await new MikroOrmWalletRepository(verifyEm).findById(walletId);
    expect(wallet?.balance.toString()).toBe("100.00");
  });
});
