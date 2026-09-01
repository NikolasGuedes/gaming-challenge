import { Body, Controller, Get, Inject, NotFoundException, Param, Post, Query } from "@nestjs/common";
import { Money } from "../../../shared-kernel/money.js";
import { CreateWalletUseCase } from "../../application/use-cases/create-wallet.use-case.js";
import { ReconcileWalletUseCase } from "../../application/use-cases/reconcile-wallet.use-case.js";
import { WALLET_REPOSITORY, type WalletRepository } from "../../application/ports/wallet.repository.js";
import { CreateWalletDto } from "./dto/create-wallet.dto.js";
import { toWalletResponseDto, WalletResponseDto } from "./dto/wallet-response.dto.js";
import { toLedgerResponseDto, LedgerResponseDto } from "./dto/ledger-response.dto.js";
import { toReconciliationResponseDto, ReconciliationResponseDto } from "./dto/reconciliation-response.dto.js";

@Controller("wallets")
export class WalletController {
  constructor(
    private readonly createWalletUseCase: CreateWalletUseCase,
    private readonly reconcileWalletUseCase: ReconcileWalletUseCase,
    @Inject(WALLET_REPOSITORY) private readonly walletRepository: WalletRepository,
  ) {}

  @Post()
  async create(@Body() dto: CreateWalletDto): Promise<WalletResponseDto> {
    const initialBalance = Money.from({ amount: dto.initialBalance ?? "0.00", currency: dto.currency });
    const wallet = await this.createWalletUseCase.execute({
      playerId: dto.playerId,
      currency: dto.currency,
      initialBalance,
    });
    return toWalletResponseDto(wallet);
  }

  @Get(":walletId")
  async getWallet(@Param("walletId") walletId: string): Promise<WalletResponseDto> {
    const wallet = await this.walletRepository.findById(walletId);
    if (!wallet) {
      throw new NotFoundException(`Wallet ${walletId} not found`);
    }
    return toWalletResponseDto(wallet);
  }

  @Get(":walletId/ledger")
  async getLedger(
    @Param("walletId") walletId: string,
    @Query("after") after?: string,
    @Query("limit") limit?: string,
  ): Promise<LedgerResponseDto> {
    const parsedLimit = limit ? Math.min(Math.max(parseInt(limit, 10), 1), 100) : 20;
    const entries = await this.walletRepository.listLedgerEntries(walletId, { after, limit: parsedLimit });
    return toLedgerResponseDto(entries, parsedLimit);
  }

  @Post(":walletId/reconciliation")
  async reconcile(@Param("walletId") walletId: string): Promise<ReconciliationResponseDto> {
    const result = await this.reconcileWalletUseCase.execute(walletId);
    return toReconciliationResponseDto(result);
  }
}
