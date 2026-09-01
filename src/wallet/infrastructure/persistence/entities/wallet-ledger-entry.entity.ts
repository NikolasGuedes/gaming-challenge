import { defineEntity, p } from "@mikro-orm/postgresql";

const WalletLedgerEntrySchema = defineEntity({
  name: "WalletLedgerEntryEntity",
  tableName: "wallet_ledger_entries",
  properties: {
    id: p.uuid().primary(),
    walletId: p.uuid(),
    transactionId: p.uuid(),
    direction: p.string(),
    amount: p.decimal().precision(19).scale(4),
    currency: p.string().length(3),
    balanceBefore: p.decimal().precision(19).scale(4),
    balanceAfter: p.decimal().precision(19).scale(4),
    createdAt: p.datetime().onCreate(() => new Date()),
  },
});

export class WalletLedgerEntryEntity extends WalletLedgerEntrySchema.class {}
WalletLedgerEntrySchema.setClass(WalletLedgerEntryEntity);
