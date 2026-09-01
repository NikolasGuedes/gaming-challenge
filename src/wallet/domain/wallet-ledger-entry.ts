import { Money } from "../../shared-kernel/money.js";

export type LedgerDirection = "DEBIT" | "CREDIT";

export class WalletLedgerEntry {
  private constructor(
    readonly id: string,
    readonly walletId: string,
    readonly transactionId: string,
    readonly direction: LedgerDirection,
    readonly amount: Money,
    readonly balanceBefore: Money,
    readonly balanceAfter: Money,
    readonly createdAt: Date,
  ) {}

  static create(input: {
    id: string;
    walletId: string;
    transactionId: string;
    direction: LedgerDirection;
    amount: Money;
    balanceBefore: Money;
    balanceAfter: Money;
    createdAt?: Date;
  }): WalletLedgerEntry {
    const expected =
      input.direction === "CREDIT"
        ? input.balanceBefore.add(input.amount)
        : input.balanceBefore.subtract(input.amount);
    if (!expected.equals(input.balanceAfter)) {
      throw new Error(
        `Ledger arithmetic mismatch: expected balanceAfter ${expected.toString()}, got ${input.balanceAfter.toString()}`,
      );
    }
    return new WalletLedgerEntry(
      input.id,
      input.walletId,
      input.transactionId,
      input.direction,
      input.amount,
      input.balanceBefore,
      input.balanceAfter,
      input.createdAt ?? new Date(),
    );
  }

  /** Rebuilds a trusted row coming from the database — no re-validation. */
  static rehydrate(input: {
    id: string;
    walletId: string;
    transactionId: string;
    direction: LedgerDirection;
    amount: Money;
    balanceBefore: Money;
    balanceAfter: Money;
    createdAt: Date;
  }): WalletLedgerEntry {
    return new WalletLedgerEntry(
      input.id,
      input.walletId,
      input.transactionId,
      input.direction,
      input.amount,
      input.balanceBefore,
      input.balanceAfter,
      input.createdAt,
    );
  }
}
