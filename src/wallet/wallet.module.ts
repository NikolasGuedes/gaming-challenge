import { Module } from "@nestjs/common";
import { CreateWalletUseCase } from "./application/use-cases/create-wallet.use-case.js";
import { WALLET_REPOSITORY } from "./application/ports/wallet.repository.js";
import { MikroOrmWalletRepository } from "./infrastructure/persistence/repositories/wallet.repository.js";
import { WalletController } from "./infrastructure/http/wallet.controller.js";
import { WAGER_TRANSACTION_REPOSITORY } from "../wagering/application/ports/wager-transaction.repository.js";
import { MikroOrmWagerTransactionRepository } from "../wagering/infrastructure/persistence/repositories/wager-transaction.repository.js";

@Module({
  controllers: [WalletController],
  providers: [
    CreateWalletUseCase,
    { provide: WALLET_REPOSITORY, useClass: MikroOrmWalletRepository },
    // CreateWalletUseCase persists the internal OPENING WagerTransaction, so this
    // module needs its own binding for this token — NestJS providers are
    // module-scoped, and WageringModule (Task 12) imports WalletModule, so the
    // reverse import would be circular. MikroOrmWagerTransactionRepository is a
    // stateless wrapper over the injected, context-scoped EntityManager, so
    // registering it again here (alongside WageringModule's own binding) is safe.
    { provide: WAGER_TRANSACTION_REPOSITORY, useClass: MikroOrmWagerTransactionRepository },
  ],
  exports: [WALLET_REPOSITORY],
})
export class WalletModule {}
