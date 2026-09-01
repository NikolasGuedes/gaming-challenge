import { Inject, Injectable, Logger, NotFoundException } from "@nestjs/common";
import { Money } from "../../../shared-kernel/money.js";
import { WALLET_REPOSITORY, type WalletRepository } from "../ports/wallet.repository.js";

export interface ReconciliationResult {
  walletId: string;
  storedBalance: Money;
  calculatedBalance: Money;
  difference: Money;
  consistent: boolean;
  checkedEntries: number;
}

@Injectable()
export class ReconcileWalletUseCase {
  private readonly logger = new Logger(ReconcileWalletUseCase.name);

  constructor(@Inject(WALLET_REPOSITORY) private readonly walletRepository: WalletRepository) {}

  async execute(walletId: string): Promise<ReconciliationResult> {
    const wallet = await this.walletRepository.findById(walletId);
    if (!wallet) {
      throw new NotFoundException(`Wallet ${walletId} not found`);
    }

    const { balance: calculatedBalance, count } = await this.walletRepository.sumLedgerEntries(
      walletId,
      wallet.currency,
    );
    const difference = wallet.balance.subtract(calculatedBalance);
    const consistent = difference.isZero();

    if (!consistent) {
      this.logger.error(
        `Reconciliation drift on wallet ${walletId}: stored=${wallet.balance.toString()} calculated=${calculatedBalance.toString()} difference=${difference.toString()}`,
      );
    }

    return {
      walletId,
      storedBalance: wallet.balance,
      calculatedBalance,
      difference,
      consistent,
      checkedEntries: count,
    };
  }
}
