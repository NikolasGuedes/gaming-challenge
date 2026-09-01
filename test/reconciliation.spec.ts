import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { Money } from "../src/shared-kernel/money.js";
import { CreateWalletUseCase } from "../src/wallet/application/use-cases/create-wallet.use-case.js";
import { ReconcileWalletUseCase } from "../src/wallet/application/use-cases/reconcile-wallet.use-case.js";
import { MikroOrmWalletRepository } from "../src/wallet/infrastructure/persistence/repositories/wallet.repository.js";
import { MikroOrmWagerTransactionRepository } from "../src/wagering/infrastructure/persistence/repositories/wager-transaction.repository.js";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env.js";

describe("ReconcileWalletUseCase", () => {
  let db: TestDatabase;

  beforeAll(async () => {
    db = await startTestDatabase();
  }, 60_000);

  afterAll(async () => {
    await stopTestDatabase(db);
  });

  it("reports consistent: true when balance matches the ledger", async () => {
    const em = db.orm.em.fork({ useContext: true });
    const wallet = await new CreateWalletUseCase(
      em,
      new MikroOrmWalletRepository(em),
      new MikroOrmWagerTransactionRepository(em),
    ).execute({
      playerId: "player-recon-1",
      currency: "BRL",
      initialBalance: Money.from({ amount: "80.00", currency: "BRL" }),
    });

    const reconcileEm = db.orm.em.fork();
    const result = await new ReconcileWalletUseCase(new MikroOrmWalletRepository(reconcileEm)).execute(wallet.id);

    expect(result.consistent).toBe(true);
    expect(result.storedBalance.toString()).toBe("80.00");
    expect(result.calculatedBalance.toString()).toBe("80.00");
    expect(result.difference.isZero()).toBe(true);
    expect(result.checkedEntries).toBe(1);
  });

  it("reports consistent: false and the exact drift when the stored balance was corrupted", async () => {
    const em = db.orm.em.fork({ useContext: true });
    const wallet = await new CreateWalletUseCase(
      em,
      new MikroOrmWalletRepository(em),
      new MikroOrmWagerTransactionRepository(em),
    ).execute({
      playerId: "player-recon-2",
      currency: "BRL",
      initialBalance: Money.from({ amount: "80.00", currency: "BRL" }),
    });

    // Simulate a bug that corrupted the materialized balance without touching the ledger.
    const corruptEm = db.orm.em.fork();
    await corruptEm.getConnection().execute(`update wallets set balance = '999.00' where id = ?`, [wallet.id]);

    const reconcileEm = db.orm.em.fork();
    const result = await new ReconcileWalletUseCase(new MikroOrmWalletRepository(reconcileEm)).execute(wallet.id);

    expect(result.consistent).toBe(false);
    expect(result.storedBalance.toString()).toBe("999.00");
    expect(result.calculatedBalance.toString()).toBe("80.00");
    expect(result.difference.toString()).toBe("919.00");
  });
});
