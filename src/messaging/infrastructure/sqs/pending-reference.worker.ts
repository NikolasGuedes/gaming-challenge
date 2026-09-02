import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import { MikroORM } from "@mikro-orm/postgresql";
import { ProcessWagerUseCase } from "../../../wagering/application/use-cases/process-wager.use-case.js";
import { MikroOrmWagerTransactionRepository } from "../../../wagering/infrastructure/persistence/repositories/wager-transaction.repository.js";
import { MikroOrmWalletRepository } from "../../../wallet/infrastructure/persistence/repositories/wallet.repository.js";
import { MikroOrmOutboxRepository } from "../persistence/repositories/outbox.repository.js";

const POLL_INTERVAL_MS = 2_000;
const BATCH_SIZE = 20;

@Injectable()
export class PendingReferenceWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PendingReferenceWorker.name);
  private timer: ReturnType<typeof setInterval> | null = null;
  private ticking = false;

  constructor(private readonly orm: MikroORM) {}

  onModuleInit(): void {
    this.timer = setInterval(() => void this.tick(), POLL_INTERVAL_MS);
  }

  onModuleDestroy(): void {
    if (this.timer) clearInterval(this.timer);
  }

  async tick(now = new Date()): Promise<number> {
    if (this.ticking) return 0;
    this.ticking = true;
    try {
      const lookupRepo = new MikroOrmWagerTransactionRepository(this.orm.em.fork());
      const ids = await lookupRepo.findDuePendingReferenceIds(now, BATCH_SIZE);
      for (const id of ids) {
        const em = this.orm.em.fork({ useContext: true });
        const useCase = new ProcessWagerUseCase(
          em,
          new MikroOrmWalletRepository(em),
          new MikroOrmWagerTransactionRepository(em),
          new MikroOrmOutboxRepository(em, this.orm),
        );
        try {
          await useCase.retryPendingReference(id, now);
        } catch (error) {
          this.logger.error(JSON.stringify({ event: "pending_reference_retry_failed", transactionId: id }), error as Error);
        }
      }
      return ids.length;
    } finally {
      this.ticking = false;
    }
  }
}
