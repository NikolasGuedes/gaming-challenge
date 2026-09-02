import { Money } from "../../../shared-kernel/money.js";
import { Wallet } from "../../domain/wallet.js";
import { WalletLedgerEntry } from "../../domain/wallet-ledger-entry.js";

export interface WalletRepository {
  findById(id: string): Promise<Wallet | null>;
  /** Must be called inside an active transaction — issues SELECT ... FOR UPDATE. */
  findByIdForUpdate(id: string): Promise<Wallet | null>;
  findByPlayerAndCurrency(playerId: string, currency: string): Promise<Wallet | null>;
  save(wallet: Wallet): Promise<void>;
  appendLedgerEntry(entry: WalletLedgerEntry): Promise<void>;
  listLedgerEntries(
    walletId: string,
    cursor: { after?: { createdAt: Date; id: string }; limit: number },
  ): Promise<WalletLedgerEntry[]>;
  /** Recomputes the balance directly in SQL from every ledger row — the reconciliation source of truth. */
  sumLedgerEntries(walletId: string, currency: string): Promise<{ balance: Money; count: number }>;
}

export const WALLET_REPOSITORY = Symbol("WALLET_REPOSITORY");
