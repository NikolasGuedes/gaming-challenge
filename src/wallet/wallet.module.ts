import { Module } from "@nestjs/common";
import { CreateWalletUseCase } from "./application/use-cases/create-wallet.use-case.js";
import { WALLET_REPOSITORY } from "./application/ports/wallet.repository.js";
import { MikroOrmWalletRepository } from "./infrastructure/persistence/repositories/wallet.repository.js";
import { WalletController } from "./infrastructure/http/wallet.controller.js";

@Module({
  controllers: [WalletController],
  providers: [
    CreateWalletUseCase,
    { provide: WALLET_REPOSITORY, useClass: MikroOrmWalletRepository },
  ],
  exports: [WALLET_REPOSITORY],
})
export class WalletModule {}
