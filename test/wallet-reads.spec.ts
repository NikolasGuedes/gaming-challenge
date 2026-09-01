import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { Money } from "../src/shared-kernel/money.js";
import { CreateWalletUseCase } from "../src/wallet/application/use-cases/create-wallet.use-case.js";
import { ReconcileWalletUseCase } from "../src/wallet/application/use-cases/reconcile-wallet.use-case.js";
import { MikroOrmWalletRepository } from "../src/wallet/infrastructure/persistence/repositories/wallet.repository.js";
import { MikroOrmWagerTransactionRepository } from "../src/wagering/infrastructure/persistence/repositories/wager-transaction.repository.js";
import { WalletController } from "../src/wallet/infrastructure/http/wallet.controller.js";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env.js";

describe("WalletController — reads", () => {
  let db: TestDatabase;

  beforeAll(async () => {
    db = await startTestDatabase();
  }, 60_000);

  afterAll(async () => {
    await stopTestDatabase(db);
  });

  it("GET /wallets/:walletId returns the wallet", async () => {
    const em = db.orm.em.fork({ useContext: true });
    const walletRepo = new MikroOrmWalletRepository(em);
    const createUseCase = new CreateWalletUseCase(em, walletRepo, new MikroOrmWagerTransactionRepository(em));
    const wallet = await createUseCase.execute({
      playerId: "player-read-1",
      currency: "BRL",
      initialBalance: Money.from({ amount: "50.00", currency: "BRL" }),
    });

    const readEm = db.orm.em.fork();
    const readRepo = new MikroOrmWalletRepository(readEm);
    const controller = new WalletController(createUseCase, new ReconcileWalletUseCase(readRepo), readRepo);
    const response = await controller.getWallet(wallet.id);
    expect(response.balance).toEqual({ amount: "50.00", currency: "BRL" });
  });

  it("GET /wallets/:walletId/ledger returns the OPENING entry", async () => {
    const em = db.orm.em.fork({ useContext: true });
    const walletRepo = new MikroOrmWalletRepository(em);
    const createUseCase = new CreateWalletUseCase(em, walletRepo, new MikroOrmWagerTransactionRepository(em));
    const wallet = await createUseCase.execute({
      playerId: "player-read-2",
      currency: "BRL",
      initialBalance: Money.from({ amount: "50.00", currency: "BRL" }),
    });

    const readEm = db.orm.em.fork();
    const readRepo = new MikroOrmWalletRepository(readEm);
    const controller = new WalletController(createUseCase, new ReconcileWalletUseCase(readRepo), readRepo);
    const ledger = await controller.getLedger(wallet.id);
    expect(ledger.entries).toHaveLength(1);
    expect(ledger.entries[0].direction).toBe("CREDIT");
    expect(ledger.nextCursor).toBeNull();
  });
});
