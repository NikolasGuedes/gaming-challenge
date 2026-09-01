import { Body, Controller, Post } from "@nestjs/common";
import { Money } from "../../../shared-kernel/money.js";
import { CreateWalletUseCase } from "../../application/use-cases/create-wallet.use-case.js";
import { CreateWalletDto } from "./dto/create-wallet.dto.js";
import { toWalletResponseDto, WalletResponseDto } from "./dto/wallet-response.dto.js";

@Controller("wallets")
export class WalletController {
  constructor(private readonly createWalletUseCase: CreateWalletUseCase) {}

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
}
