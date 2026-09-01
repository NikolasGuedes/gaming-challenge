import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { WagerTransaction } from "../src/wagering/domain/wager-transaction.js";
import { Money } from "../src/shared-kernel/money.js";
import { MikroOrmWagerTransactionRepository } from "../src/wagering/infrastructure/persistence/repositories/wager-transaction.repository.js";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env.js";

describe("MikroOrmWagerTransactionRepository — DB-level uniqueness", () => {
  let db: TestDatabase;

  beforeAll(async () => {
    db = await startTestDatabase();
    await db.orm.em.getConnection().execute(
      `insert into wallets (id, player_id, currency) values (?, 'player-x', 'BRL')`,
      ["33333333-3333-3333-3333-333333333333"],
    );
  }, 60_000);

  afterAll(async () => {
    await stopTestDatabase(db);
  });

  it("rejects a second insert with the same (providerId, externalTransactionId)", async () => {
    const em = db.orm.em.fork();
    const repo = new MikroOrmWagerTransactionRepository(em);

    const first = WagerTransaction.create({
      id: "44444444-4444-4444-4444-444444444444",
      walletId: "33333333-3333-3333-3333-333333333333",
      externalTransactionId: "bet-1",
      providerId: "provider-a",
      idempotencyKey: "idem-1",
      payloadHash: "hash-1",
      kind: "BET",
      amount: Money.from({ amount: "25.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });
    await repo.save(first);
    await em.flush();

    const duplicate = WagerTransaction.create({
      id: "55555555-5555-5555-5555-555555555555",
      walletId: "33333333-3333-3333-3333-333333333333",
      externalTransactionId: "bet-1", // same provider + external id
      providerId: "provider-a",
      idempotencyKey: "idem-2",
      payloadHash: "hash-2",
      kind: "BET",
      amount: Money.from({ amount: "25.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });
    await repo.save(duplicate);
    await expect(em.flush()).rejects.toThrow();
  });
});
