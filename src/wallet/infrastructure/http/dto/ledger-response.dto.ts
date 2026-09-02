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

export function encodeLedgerCursor(entry: WalletLedgerEntry): string {
  return Buffer.from(JSON.stringify({ createdAt: entry.createdAt.toISOString(), id: entry.id })).toString("base64url");
}

export function decodeLedgerCursor(cursor: string): { createdAt: Date; id: string } {
  try {
    const parsed = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8")) as {
      createdAt?: unknown;
      id?: unknown;
    };
    const createdAt = new Date(String(parsed.createdAt));
    if (typeof parsed.id !== "string" || parsed.id.length === 0 || Number.isNaN(createdAt.getTime())) {
      throw new Error("invalid cursor payload");
    }
    return { createdAt, id: parsed.id };
  } catch {
    throw new Error("Invalid ledger cursor");
  }
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
    nextCursor: entries.length === limit ? encodeLedgerCursor(entries[entries.length - 1]) : null,
  };
}
