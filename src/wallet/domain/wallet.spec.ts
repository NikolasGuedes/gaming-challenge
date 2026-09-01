import { describe, expect, it } from "bun:test";
import { Money } from "../../shared-kernel/money";
import { FailureCode } from "../../shared-kernel/failure-code";
import { Wallet } from "./wallet";

describe("Wallet.open", () => {
  it("does not create an opening entry when initial balance is zero", () => {
    const { wallet, openingEntry } = Wallet.open({
      id: "wallet-1",
      playerId: "player-1",
      currency: "BRL",
      initialBalance: Money.zero("BRL"),
      openingTransactionId: "opening-1",
    });
    expect(wallet.balance.isZero()).toBe(true);
    expect(wallet.version).toBe(1);
    expect(openingEntry).toBeNull();
  });

  it("credits the initial balance atomically when greater than zero", () => {
    const { wallet, openingEntry } = Wallet.open({
      id: "wallet-1",
      playerId: "player-1",
      currency: "BRL",
      initialBalance: Money.from({ amount: "100.00", currency: "BRL" }),
      openingTransactionId: "opening-1",
    });
    expect(wallet.balance.toString()).toBe("100.00");
    expect(openingEntry?.direction).toBe("CREDIT");
    expect(openingEntry?.balanceBefore.toString()).toBe("0.00");
    expect(openingEntry?.balanceAfter.toString()).toBe("100.00");
  });
});

describe("Wallet.debit", () => {
  it("processes a debit when funds are sufficient", () => {
    const { wallet: opened } = Wallet.open({
      id: "wallet-1", playerId: "player-1", currency: "BRL",
      initialBalance: Money.from({ amount: "100.00", currency: "BRL" }),
      openingTransactionId: "opening-1",
    });
    const result = opened.debit({ amount: Money.from({ amount: "80.00", currency: "BRL" }), transactionId: "tx-1" });
    expect(result.status).toBe("PROCESSED");
    if (result.status === "PROCESSED") {
      expect(result.wallet.balance.toString()).toBe("20.00");
      expect(result.wallet.version).toBe(opened.version + 1);
      expect(result.entry.direction).toBe("DEBIT");
      expect(result.entry.balanceBefore.toString()).toBe("100.00");
      expect(result.entry.balanceAfter.toString()).toBe("20.00");
    }
    // original instance is untouched — immutability
    expect(opened.balance.toString()).toBe("100.00");
  });

  it("rejects a debit with insufficient funds instead of throwing", () => {
    const { wallet: opened } = Wallet.open({
      id: "wallet-1", playerId: "player-1", currency: "BRL",
      initialBalance: Money.from({ amount: "20.00", currency: "BRL" }),
      openingTransactionId: "opening-1",
    });
    const result = opened.debit({ amount: Money.from({ amount: "80.00", currency: "BRL" }), transactionId: "tx-2" });
    expect(result).toEqual({ status: "REJECTED", failureCode: FailureCode.INSUFFICIENT_FUNDS });
    expect(opened.balance.toString()).toBe("20.00");
  });

  it("rejects a debit in the wrong currency", () => {
    const { wallet: opened } = Wallet.open({
      id: "wallet-1", playerId: "player-1", currency: "BRL",
      initialBalance: Money.from({ amount: "100.00", currency: "BRL" }),
      openingTransactionId: "opening-1",
    });
    const result = opened.debit({ amount: Money.from({ amount: "10.00", currency: "USD" }), transactionId: "tx-3" });
    expect(result).toEqual({ status: "REJECTED", failureCode: FailureCode.CURRENCY_MISMATCH });
  });
});

describe("Wallet.credit", () => {
  it("processes a credit and increases the balance", () => {
    const { wallet: opened } = Wallet.open({
      id: "wallet-1", playerId: "player-1", currency: "BRL",
      initialBalance: Money.from({ amount: "20.00", currency: "BRL" }),
      openingTransactionId: "opening-1",
    });
    const result = opened.credit({ amount: Money.from({ amount: "150.00", currency: "BRL" }), transactionId: "tx-win" });
    expect(result.status).toBe("PROCESSED");
    if (result.status === "PROCESSED") {
      expect(result.wallet.balance.toString()).toBe("170.00");
      expect(result.entry.direction).toBe("CREDIT");
    }
  });
});
