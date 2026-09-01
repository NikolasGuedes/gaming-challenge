import { ConflictException, Inject, Injectable } from "@nestjs/common";
import { EntityManager } from "@mikro-orm/postgresql";
import { Money } from "../../../shared-kernel/money.js";
import { computePayloadHash } from "../../../wagering/domain/payload-hash.js";
import { WagerTransaction } from "../../../wagering/domain/wager-transaction.js";
import {
  WAGER_TRANSACTION_REPOSITORY,
  type WagerTransactionRepository,
} from "../../../wagering/application/ports/wager-transaction.repository.js";
import { Wallet } from "../../domain/wallet.js";
import { WALLET_REPOSITORY, type WalletRepository } from "../ports/wallet.repository.js";

@Injectable()
export class CreateWalletUseCase {
  constructor(
    private readonly em: EntityManager,
    @Inject(WALLET_REPOSITORY) private readonly walletRepository: WalletRepository,
    @Inject(WAGER_TRANSACTION_REPOSITORY)
    private readonly wagerTransactionRepository: WagerTransactionRepository,
  ) {}

  async execute(input: { playerId: string; currency: string; initialBalance: Money }): Promise<Wallet> {
    return this.em.transactional(async () => {
      const existing = await this.walletRepository.findByPlayerAndCurrency(input.playerId, input.currency);
      if (existing) {
        throw new ConflictException(
          `Wallet already exists for player ${input.playerId} in ${input.currency}`,
        );
      }

      const walletId = crypto.randomUUID();
      const openingTransactionId = crypto.randomUUID();
      const { wallet, openingEntry } = Wallet.open({
        id: walletId,
        playerId: input.playerId,
        currency: input.currency,
        initialBalance: input.initialBalance,
        openingTransactionId,
      });

      // Insert order matters here: wallet_ledger_entries has FK columns pointing at
      // both wallets and wager_transactions, and wager_transactions has a FK pointing
      // at wallets — none of this is expressed as a MikroORM relation (these are plain
      // UUID columns), so the unit of work has no dependency graph to compute a safe
      // insert order from. We flush wallet, then the OPENING transaction, before
      // persisting the ledger entry that references both.
      await this.walletRepository.save(wallet);
      await this.em.flush();

      if (openingEntry) {
        const openingTx = WagerTransaction.create({
          id: openingTransactionId,
          walletId,
          externalTransactionId: `opening-${walletId}`,
          providerId: "internal",
          idempotencyKey: `opening-${walletId}`,
          payloadHash: computePayloadHash({ walletId, initialBalance: input.initialBalance.toJSON() }),
          kind: "OPENING",
          amount: input.initialBalance,
          referenceExternalTransactionId: null,
        }).markProcessed(wallet.balance);
        await this.wagerTransactionRepository.save(openingTx);
        await this.em.flush();

        await this.walletRepository.appendLedgerEntry(openingEntry);
      }

      return wallet;
    });
  }
}
