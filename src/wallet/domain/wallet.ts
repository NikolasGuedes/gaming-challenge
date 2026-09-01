import { Money } from "../../shared-kernel/money.js";
import { FailureCode } from "../../shared-kernel/failure-code.js";
import { WalletLedgerEntry } from "./wallet-ledger-entry.js";

export type WalletApplyResult =
  | { status: "PROCESSED"; wallet: Wallet; entry: WalletLedgerEntry }
  | { status: "REJECTED"; failureCode: FailureCode };

export class Wallet {
  private constructor(
    readonly id: string,
    readonly playerId: string,
    readonly currency: string,
    readonly balance: Money,
    readonly version: number,
  ) {}

  static open(input: {
    id: string;
    playerId: string;
    currency: string;
    initialBalance: Money;
    openingTransactionId: string;
  }): { wallet: Wallet; openingEntry: WalletLedgerEntry | null } {
    const zero = Money.zero(input.currency);
    if (input.initialBalance.currency !== input.currency) {
      throw new Error(
        `Initial balance currency ${input.initialBalance.currency} does not match wallet currency ${input.currency}`,
      );
    }
    if (!input.initialBalance.isPositive()) {
      return {
        wallet: new Wallet(input.id, input.playerId, input.currency, zero, 1),
        openingEntry: null,
      };
    }
    const balanceAfter = zero.add(input.initialBalance);
    const openingEntry = WalletLedgerEntry.create({
      id: crypto.randomUUID(),
      walletId: input.id,
      transactionId: input.openingTransactionId,
      direction: "CREDIT",
      amount: input.initialBalance,
      balanceBefore: zero,
      balanceAfter,
    });
    return {
      wallet: new Wallet(input.id, input.playerId, input.currency, balanceAfter, 1),
      openingEntry,
    };
  }

  static rehydrate(input: {
    id: string;
    playerId: string;
    currency: string;
    balance: Money;
    version: number;
  }): Wallet {
    return new Wallet(input.id, input.playerId, input.currency, input.balance, input.version);
  }

  debit(input: { amount: Money; transactionId: string }): WalletApplyResult {
    if (input.amount.currency !== this.currency) {
      return { status: "REJECTED", failureCode: FailureCode.CURRENCY_MISMATCH };
    }
    if (this.balance.isLessThan(input.amount)) {
      return { status: "REJECTED", failureCode: FailureCode.INSUFFICIENT_FUNDS };
    }
    const balanceAfter = this.balance.subtract(input.amount);
    const entry = WalletLedgerEntry.create({
      id: crypto.randomUUID(),
      walletId: this.id,
      transactionId: input.transactionId,
      direction: "DEBIT",
      amount: input.amount,
      balanceBefore: this.balance,
      balanceAfter,
    });
    const wallet = new Wallet(this.id, this.playerId, this.currency, balanceAfter, this.version + 1);
    return { status: "PROCESSED", wallet, entry };
  }

  credit(input: { amount: Money; transactionId: string }): WalletApplyResult {
    if (input.amount.currency !== this.currency) {
      return { status: "REJECTED", failureCode: FailureCode.CURRENCY_MISMATCH };
    }
    const balanceAfter = this.balance.add(input.amount);
    const entry = WalletLedgerEntry.create({
      id: crypto.randomUUID(),
      walletId: this.id,
      transactionId: input.transactionId,
      direction: "CREDIT",
      amount: input.amount,
      balanceBefore: this.balance,
      balanceAfter,
    });
    const wallet = new Wallet(this.id, this.playerId, this.currency, balanceAfter, this.version + 1);
    return { status: "PROCESSED", wallet, entry };
  }
}
