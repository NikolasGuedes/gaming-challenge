import { Module } from "@nestjs/common";
import { ProcessWagerUseCase } from "./application/use-cases/process-wager.use-case.js";
import { WAGER_TRANSACTION_REPOSITORY } from "./application/ports/wager-transaction.repository.js";
import { IDEMPOTENCY_KEY_REPOSITORY } from "./application/ports/idempotency-key.repository.js";
import { MikroOrmWagerTransactionRepository } from "./infrastructure/persistence/repositories/wager-transaction.repository.js";
import { MikroOrmIdempotencyKeyRepository } from "./infrastructure/persistence/repositories/idempotency-key.repository.js";
import { IdempotencyService } from "./infrastructure/http/idempotency.service.js";
import { WageringController } from "./infrastructure/http/wagering.controller.js";
import { WalletModule } from "../wallet/wallet.module.js";
import { MessagingModule } from "../messaging/messaging.module.js";

@Module({
  imports: [WalletModule, MessagingModule],
  controllers: [WageringController],
  providers: [
    ProcessWagerUseCase,
    IdempotencyService,
    { provide: WAGER_TRANSACTION_REPOSITORY, useClass: MikroOrmWagerTransactionRepository },
    { provide: IDEMPOTENCY_KEY_REPOSITORY, useClass: MikroOrmIdempotencyKeyRepository },
  ],
  exports: [WAGER_TRANSACTION_REPOSITORY],
})
export class WageringModule {}
