import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { Money } from "../src/shared-kernel/money.js";
import { ConflictException } from "@nestjs/common";
import { FailureCode } from "../src/shared-kernel/failure-code.js";
import { CreateWalletUseCase } from "../src/wallet/application/use-cases/create-wallet.use-case.js";
import { MikroOrmWalletRepository } from "../src/wallet/infrastructure/persistence/repositories/wallet.repository.js";
import { MikroOrmWagerTransactionRepository } from "../src/wagering/infrastructure/persistence/repositories/wager-transaction.repository.js";
import { MikroOrmOutboxRepository } from "../src/messaging/infrastructure/persistence/repositories/outbox.repository.js";
import { ProcessWagerUseCase, ProcessWagerInput } from "../src/wagering/application/use-cases/process-wager.use-case.js";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env.js";

let db: TestDatabase;

function freshUseCase(): ProcessWagerUseCase {
  const em = db.orm.em.fork({ useContext: true });
  return new ProcessWagerUseCase(
    em,
    new MikroOrmWalletRepository(em),
    new MikroOrmWagerTransactionRepository(em),
    new MikroOrmOutboxRepository(em, db.orm),
  );
}

async function seedWallet(playerId: string, initial: string): Promise<string> {
  const em = db.orm.em.fork({ useContext: true });
  const wallet = await new CreateWalletUseCase(
    em,
    new MikroOrmWalletRepository(em),
    new MikroOrmWagerTransactionRepository(em),
  ).execute({ playerId, currency: "BRL", initialBalance: Money.from({ amount: initial, currency: "BRL" }) });
  return wallet.id;
}

describe("Concurrency — hot wallet, multi-instance, mass duplication", () => {
  beforeAll(async () => {
    db = await startTestDatabase();
  }, 60_000);

  afterAll(async () => {
    await stopTestDatabase(db);
  });

  it("section 8: two concurrent 80.00 BETs against a 100.00 balance settle to exactly 1 PROCESSED + 1 REJECTED", async () => {
    const walletId = await seedWallet("player-hot-1", "100.00");

    const request = (externalId: string): ProcessWagerInput => ({
      externalTransactionId: externalId,
      providerId: "provider-hot",
      idempotencyKey: `idem-${externalId}`,
      payloadHash: `hash-${externalId}`,
      kind: "BET",
      walletId,
      amount: Money.from({ amount: "80.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });

    const [resultA, resultB] = await Promise.all([
      freshUseCase().execute(request("hot-bet-a")),
      freshUseCase().execute(request("hot-bet-b")),
    ]);

    const statuses = [resultA.status, resultB.status].sort();
    expect(statuses).toEqual(["PROCESSED", "REJECTED"]);

    const rejected = resultA.status === "REJECTED" ? resultA : resultB;
    if (rejected.status === "REJECTED") {
      expect(rejected.failureCode).toBe(FailureCode.INSUFFICIENT_FUNDS);
    }

    const verifyEm = db.orm.em.fork();
    const wallet = await new MikroOrmWalletRepository(verifyEm).findById(walletId);
    expect(wallet?.balance.toString()).toBe("20.00");
    const ledger = await new MikroOrmWalletRepository(verifyEm).listLedgerEntries(walletId, { limit: 100 });
    expect(ledger.filter((entry) => entry.direction === "DEBIT")).toHaveLength(1);
  }, 20_000);

  it("3+ concurrent instances: two race on the same wallet while a third processes an unrelated wallet", async () => {
    const hotWalletId = await seedWallet("player-hot-2", "100.00");
    const otherWalletId = await seedWallet("player-other-1", "50.00");

    const [hotA, hotB, other] = await Promise.all([
      freshUseCase().execute({
        externalTransactionId: "multi-hot-a",
        providerId: "provider-multi",
        idempotencyKey: "idem-multi-hot-a",
        payloadHash: "hash-multi-hot-a",
        kind: "BET",
        walletId: hotWalletId,
        amount: Money.from({ amount: "80.00", currency: "BRL" }),
        referenceExternalTransactionId: null,
      }),
      freshUseCase().execute({
        externalTransactionId: "multi-hot-b",
        providerId: "provider-multi",
        idempotencyKey: "idem-multi-hot-b",
        payloadHash: "hash-multi-hot-b",
        kind: "BET",
        walletId: hotWalletId,
        amount: Money.from({ amount: "80.00", currency: "BRL" }),
        referenceExternalTransactionId: null,
      }),
      freshUseCase().execute({
        externalTransactionId: "multi-other",
        providerId: "provider-multi",
        idempotencyKey: "idem-multi-other",
        payloadHash: "hash-multi-other",
        kind: "BET",
        walletId: otherWalletId,
        amount: Money.from({ amount: "10.00", currency: "BRL" }),
        referenceExternalTransactionId: null,
      }),
    ]);

    expect([hotA.status, hotB.status].sort()).toEqual(["PROCESSED", "REJECTED"]);
    expect(other.status).toBe("PROCESSED"); // unrelated wallet never blocked by the hot-wallet contention

    const verifyEm = db.orm.em.fork();
    const otherWallet = await new MikroOrmWalletRepository(verifyEm).findById(otherWalletId);
    expect(otherWallet?.balance.toString()).toBe("40.00");
  }, 20_000);

  it("the same bet sent 50 times in parallel produces exactly one debit and 49 idempotent replays", async () => {
    const walletId = await seedWallet("player-mass-1", "1000.00");

    const request: ProcessWagerInput = {
      externalTransactionId: "mass-bet-1",
      providerId: "provider-mass",
      idempotencyKey: "idem-mass-1",
      payloadHash: "hash-mass-1",
      kind: "BET",
      walletId,
      amount: Money.from({ amount: "25.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    };

    const results = await Promise.all(Array.from({ length: 50 }, () => freshUseCase().execute(request)));

    const processedFirstTime = results.filter((r) => !r.idempotentReplay);
    const replays = results.filter((r) => r.idempotentReplay);
    expect(processedFirstTime).toHaveLength(1);
    expect(replays).toHaveLength(49);
    expect(new Set(results.map((r) => r.transactionId)).size).toBe(1); // everyone agrees on the same transaction id

    const verifyEm = db.orm.em.fork();
    const wallet = await new MikroOrmWalletRepository(verifyEm).findById(walletId);
    expect(wallet?.balance.toString()).toBe("975.00"); // debited exactly once
  }, 30_000);

  it("concurrent requests reusing one idempotency key with different payloads apply exactly one debit", async () => {
    const walletId = await seedWallet("player-idem-race", "100.00");
    const request = (suffix: string, amount: string): ProcessWagerInput => ({
      externalTransactionId: `idem-race-${suffix}`,
      providerId: "provider-idem-race",
      idempotencyKey: "idem-race-shared-key",
      payloadHash: `hash-${suffix}`,
      kind: "BET",
      walletId,
      amount: Money.from({ amount, currency: "BRL" }),
      referenceExternalTransactionId: null,
    });

    const results = await Promise.allSettled([
      freshUseCase().execute(request("a", "25.00")),
      freshUseCase().execute(request("b", "50.00")),
    ]);
    expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
    const rejected = results.find((result) => result.status === "rejected");
    expect(rejected?.status).toBe("rejected");
    if (rejected?.status === "rejected") {
      expect(rejected.reason).toBeInstanceOf(ConflictException);
    }

    const repository = new MikroOrmWalletRepository(db.orm.em.fork());
    const wallet = await repository.findById(walletId);
    expect(wallet).not.toBeNull();
    expect(["50.00", "75.00"]).toContain(wallet!.balance.toString());
    const ledger = await repository.listLedgerEntries(walletId, { limit: 100 });
    expect(ledger.filter((entry) => entry.direction === "DEBIT")).toHaveLength(1);
  }, 20_000);
});
