import { WalletLedgerEntry } from "../../../domain/wallet-ledger-entry.js";

export interface LedgerResponseDto {
  entries: Array<{
    id: string;
    transactionId: string;
    direction: "DEBIT" | "CREDIT";
    amount: { amount: string; currency: string };
    balanceBefore: { amount: string; currency: string };
    balanceAfter: { amount: string; currency: string };
    createdAt: string;
  }>;
  nextCursor: string | null;
}

export function toLedgerResponseDto(entries: WalletLedgerEntry[], limit: number): LedgerResponseDto {
  return {
    entries: entries.map((entry) => ({
      id: entry.id,
      transactionId: entry.transactionId,
      direction: entry.direction,
      amount: entry.amount.toJSON(),
      balanceBefore: entry.balanceBefore.toJSON(),
      balanceAfter: entry.balanceAfter.toJSON(),
      createdAt: entry.createdAt.toISOString(),
    })),
    nextCursor: entries.length === limit ? entries[entries.length - 1].id : null,
  };
}
