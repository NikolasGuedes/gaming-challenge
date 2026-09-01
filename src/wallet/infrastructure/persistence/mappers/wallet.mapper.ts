import { Money } from "../../../../shared-kernel/money.js";
import { Wallet } from "../../../domain/wallet.js";
import { WalletLedgerEntry } from "../../../domain/wallet-ledger-entry.js";
import { WalletEntity } from "../entities/wallet.entity.js";
import { WalletLedgerEntryEntity } from "../entities/wallet-ledger-entry.entity.js";

export class WalletMapper {
  static toDomain(entity: WalletEntity): Wallet {
    return Wallet.rehydrate({
      id: entity.id,
      playerId: entity.playerId,
      currency: entity.currency,
      balance: Money.rehydrate({ amount: entity.balance, currency: entity.currency }),
      version: entity.version,
    });
  }

  static toEntity(wallet: Wallet, existing?: WalletEntity): WalletEntity {
    const entity = existing ?? new WalletEntity();
    entity.id = wallet.id;
    entity.playerId = wallet.playerId;
    entity.currency = wallet.currency;
    entity.balance = wallet.balance.toString();
    entity.version = wallet.version;
    return entity;
  }
}

export class WalletLedgerEntryMapper {
  static toDomain(entity: WalletLedgerEntryEntity): WalletLedgerEntry {
    return WalletLedgerEntry.rehydrate({
      id: entity.id,
      walletId: entity.walletId,
      transactionId: entity.transactionId,
      direction: entity.direction as "DEBIT" | "CREDIT",
      amount: Money.rehydrate({ amount: entity.amount, currency: entity.currency }),
      balanceBefore: Money.rehydrate({ amount: entity.balanceBefore, currency: entity.currency }),
      balanceAfter: Money.rehydrate({ amount: entity.balanceAfter, currency: entity.currency }),
      createdAt: entity.createdAt,
    });
  }

  static toEntity(entry: WalletLedgerEntry): WalletLedgerEntryEntity {
    const entity = new WalletLedgerEntryEntity();
    entity.id = entry.id;
    entity.walletId = entry.walletId;
    entity.transactionId = entry.transactionId;
    entity.direction = entry.direction;
    entity.amount = entry.amount.toString();
    entity.currency = entry.amount.currency;
    entity.balanceBefore = entry.balanceBefore.toString();
    entity.balanceAfter = entry.balanceAfter.toString();
    entity.createdAt = entry.createdAt;
    return entity;
  }
}
