import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { Wallet } from "../src/wallet/domain/wallet.js";
import { Money } from "../src/shared-kernel/money.js";
import { MikroOrmWalletRepository } from "../src/wallet/infrastructure/persistence/repositories/wallet.repository.js";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env.js";

describe("MikroOrmWalletRepository — FOR UPDATE lock", () => {
  let db: TestDatabase;

  beforeAll(async () => {
    db = await startTestDatabase();
  }, 60_000);

  afterAll(async () => {
    await stopTestDatabase(db);
  });

  it("serializes two concurrent findByIdForUpdate calls on the same row", async () => {
    const setupEm = db.orm.em.fork();
    const repo = new MikroOrmWalletRepository(setupEm);
    const { wallet } = Wallet.open({
      id: "11111111-1111-1111-1111-111111111111",
      playerId: "player-1",
      currency: "BRL",
      initialBalance: Money.from({ amount: "100.00", currency: "BRL" }),
      openingTransactionId: "22222222-2222-2222-2222-222222222222",
    });
    // wallets row must exist before wager_transactions (FK) and wallet_ledger_entries (FK)
    // reference it — save and flush the wallet first.
    await repo.save(wallet);
    await setupEm.flush();

    // wallet_ledger_entries has a FK to wager_transactions — insert a placeholder row
    // for the opening transaction directly so the FK is satisfiable in this narrow test.
    await setupEm.getConnection().execute(
      `insert into wager_transactions
        (id, wallet_id, external_transaction_id, provider_id, idempotency_key, payload_hash, kind, amount, currency, status)
       values (?, ?, 'opening', 'internal', ?, 'n/a', 'OPENING', 100.00, 'BRL', 'PROCESSED')`,
      ["22222222-2222-2222-2222-222222222222", wallet.id, `opening-${wallet.id}`],
    );

    const events: string[] = [];

    async function holdLockFor(ms: number): Promise<void> {
      const em = db.orm.em.fork();
      await em.transactional(async (tx) => {
        const txRepo = new MikroOrmWalletRepository(tx);
        events.push("A:acquire");
        await txRepo.findByIdForUpdate(wallet.id);
        await new Promise((resolve) => setTimeout(resolve, ms));
        events.push("A:release");
      });
    }

    async function acquireAfterWait(): Promise<void> {
      // give holdLockFor a head start so it acquires first
      await new Promise((resolve) => setTimeout(resolve, 20));
      const em = db.orm.em.fork();
      await em.transactional(async (tx) => {
        const txRepo = new MikroOrmWalletRepository(tx);
        events.push("B:waiting");
        await txRepo.findByIdForUpdate(wallet.id);
        events.push("B:acquired");
      });
    }

    await Promise.all([holdLockFor(150), acquireAfterWait()]);

    expect(events.indexOf("B:acquired")).toBeGreaterThan(events.indexOf("A:release"));
  }, 20_000);
});
