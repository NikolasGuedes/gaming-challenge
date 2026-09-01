import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { EntityManager } from "@mikro-orm/postgresql";
import { Money } from "../src/shared-kernel/money.js";
import { CreateWalletUseCase } from "../src/wallet/application/use-cases/create-wallet.use-case.js";
import { MikroOrmWalletRepository } from "../src/wallet/infrastructure/persistence/repositories/wallet.repository.js";
import { MikroOrmWagerTransactionRepository } from "../src/wagering/infrastructure/persistence/repositories/wager-transaction.repository.js";
import { WagerTransactionEntity } from "../src/wagering/infrastructure/persistence/entities/wager-transaction.entity.js";
import { WalletLedgerEntryEntity } from "../src/wallet/infrastructure/persistence/entities/wallet-ledger-entry.entity.js";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env.js";

function buildUseCase(em: EntityManager): CreateWalletUseCase {
  return new CreateWalletUseCase(
    em,
    new MikroOrmWalletRepository(em),
    new MikroOrmWagerTransactionRepository(em),
  );
}

describe("CreateWalletUseCase — OPENING transaction", () => {
  let db: TestDatabase;

  beforeAll(async () => {
    db = await startTestDatabase();
  }, 60_000);

  afterAll(async () => {
    await stopTestDatabase(db);
  });

  it("creates an OPENING ledger entry when initial balance is greater than zero", async () => {
    const em = db.orm.em.fork({ useContext: true });
    const useCase = buildUseCase(em);
    const wallet = await useCase.execute({
      playerId: "player-opening-1",
      currency: "BRL",
      initialBalance: Money.from({ amount: "100.00", currency: "BRL" }),
    });

    expect(wallet.balance.toString()).toBe("100.00");
    expect(wallet.version).toBe(1);

    const verifyEm = db.orm.em.fork();
    const ledgerRows = await verifyEm.find(WalletLedgerEntryEntity, { walletId: wallet.id });
    expect(ledgerRows).toHaveLength(1);
    expect(ledgerRows[0].direction).toBe("CREDIT");

    const openingTx = await verifyEm.findOne(WagerTransactionEntity, { walletId: wallet.id, kind: "OPENING" });
    expect(openingTx?.status).toBe("PROCESSED");
  });

  it("creates no ledger entry when initial balance is zero", async () => {
    const em = db.orm.em.fork({ useContext: true });
    const useCase = buildUseCase(em);
    const wallet = await useCase.execute({
      playerId: "player-opening-2",
      currency: "BRL",
      initialBalance: Money.zero("BRL"),
    });

    const verifyEm = db.orm.em.fork();
    const ledgerRows = await verifyEm.find(WalletLedgerEntryEntity, { walletId: wallet.id });
    expect(ledgerRows).toHaveLength(0);
  });

  it("rejects creating the same (playerId, currency) wallet twice", async () => {
    const em1 = db.orm.em.fork({ useContext: true });
    await buildUseCase(em1).execute({
      playerId: "player-opening-3",
      currency: "BRL",
      initialBalance: Money.zero("BRL"),
    });

    const em2 = db.orm.em.fork({ useContext: true });
    await expect(
      buildUseCase(em2).execute({
        playerId: "player-opening-3",
        currency: "BRL",
        initialBalance: Money.zero("BRL"),
      }),
    ).rejects.toThrow();
  });
});
