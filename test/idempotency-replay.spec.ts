import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { Money } from "../src/shared-kernel/money.js";
import { CreateWalletUseCase } from "../src/wallet/application/use-cases/create-wallet.use-case.js";
import { MikroOrmWalletRepository } from "../src/wallet/infrastructure/persistence/repositories/wallet.repository.js";
import { MikroOrmWagerTransactionRepository } from "../src/wagering/infrastructure/persistence/repositories/wager-transaction.repository.js";
import { MikroOrmOutboxRepository } from "../src/messaging/infrastructure/persistence/repositories/outbox.repository.js";
import { MikroOrmIdempotencyKeyRepository } from "../src/wagering/infrastructure/persistence/repositories/idempotency-key.repository.js";
import { ProcessWagerUseCase } from "../src/wagering/application/use-cases/process-wager.use-case.js";
import { IdempotencyService } from "../src/wagering/infrastructure/http/idempotency.service.js";
import { WageringController } from "../src/wagering/infrastructure/http/wagering.controller.js";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env.js";

describe("WageringController — HTTP idempotency", () => {
  let db: TestDatabase;
  let walletId: string;

  beforeAll(async () => {
    db = await startTestDatabase();
    const em = db.orm.em.fork();
    const wallet = await new CreateWalletUseCase(
      em,
      new MikroOrmWalletRepository(em),
      new MikroOrmWagerTransactionRepository(em),
    ).execute({ playerId: "player-http-1", currency: "BRL", initialBalance: Money.from({ amount: "100.00", currency: "BRL" }) });
    walletId = wallet.id;
  }, 60_000);

  afterAll(async () => {
    await stopTestDatabase(db);
  });

  function buildController() {
    const em = db.orm.em.fork({ useContext: true });
    const processWagerUseCase = new ProcessWagerUseCase(
      em,
      new MikroOrmWalletRepository(em),
      new MikroOrmWagerTransactionRepository(em),
      new MikroOrmOutboxRepository(em, undefined as never),
    );
    const idempotencyService = new IdempotencyService(new MikroOrmIdempotencyKeyRepository(em), em);
    return new WageringController(processWagerUseCase, idempotencyService);
  }

  const baseDto = {
    externalTransactionId: "http-bet-1",
    providerId: "provider-http",
    kind: "BET" as const,
    amount: "30.00",
    currency: "BRL",
  };

  it("requires the Idempotency-Key header", async () => {
    await expect(
      buildController().submit(undefined, { ...baseDto, walletId }),
    ).rejects.toThrow("Idempotency-Key header is required");
  });

  it("returns idempotentReplay: false on first submission, true on retry with the same key and payload", async () => {
    const first = await buildController().submit("idem-http-1", { ...baseDto, walletId });
    expect(first.idempotentReplay).toBe(false);
    expect(first.status).toBe("PROCESSED");

    const second = await buildController().submit("idem-http-1", { ...baseDto, walletId });
    expect(second.idempotentReplay).toBe(true);
    // Same exact response bytes as the first call, other than the idempotentReplay flag itself
    // (false on first submission, true on replay) — proves the cache returned the original
    // transactionId/status/balance rather than re-running any logic.
    expect(second).toEqual({ ...first, idempotentReplay: true });
  });

  it("rejects the same Idempotency-Key reused with a different payload", async () => {
    await buildController().submit("idem-http-2", { ...baseDto, walletId, externalTransactionId: "http-bet-2" });
    await expect(
      buildController().submit("idem-http-2", {
        ...baseDto,
        walletId,
        externalTransactionId: "http-bet-2",
        amount: "999.00", // different payload, same key
      }),
    ).rejects.toThrow(/different payload/);
  });
});
