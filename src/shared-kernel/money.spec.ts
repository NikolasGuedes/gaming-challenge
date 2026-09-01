import { describe, expect, it } from "bun:test";
import { Money } from "./money";
import { CurrencyMismatchError, InvalidMoneyError } from "./money.errors";

describe("Money", () => {
  it("creates from a valid decimal string", () => {
    const money = Money.from({ amount: "25.00", currency: "BRL" });
    expect(money.toString()).toBe("25.00");
    expect(money.currency).toBe("BRL");
  });

  it("accepts up to 2 decimal places", () => {
    expect(() => Money.from({ amount: "25.1", currency: "BRL" })).not.toThrow();
    expect(Money.from({ amount: "25.1", currency: "BRL" }).toString()).toBe("25.10");
  });

  it("rejects more than 2 decimal places", () => {
    expect(() => Money.from({ amount: "25.123", currency: "BRL" })).toThrow(InvalidMoneyError);
  });

  it("rejects negative amounts as entry input", () => {
    expect(() => Money.from({ amount: "-20.00", currency: "BRL" })).toThrow(InvalidMoneyError);
  });

  it("rejects scientific notation", () => {
    expect(() => Money.from({ amount: "1e10", currency: "BRL" })).toThrow(InvalidMoneyError);
  });

  it("rejects NaN, Infinity and empty string", () => {
    expect(() => Money.from({ amount: "NaN", currency: "BRL" })).toThrow(InvalidMoneyError);
    expect(() => Money.from({ amount: "Infinity", currency: "BRL" })).toThrow(InvalidMoneyError);
    expect(() => Money.from({ amount: "", currency: "BRL" })).toThrow(InvalidMoneyError);
  });

  it("zero() builds a zero amount in the given currency", () => {
    expect(Money.zero("BRL").isZero()).toBe(true);
  });

  it("is immutable — operations return new instances", () => {
    const balance = Money.from({ amount: "100.00", currency: "BRL" });
    const bet = Money.from({ amount: "25.00", currency: "BRL" });
    const result = balance.subtract(bet);
    expect(balance.toString()).toBe("100.00");
    expect(result.toString()).toBe("75.00");
  });

  it("add/subtract validate matching currency", () => {
    const brl = Money.from({ amount: "10.00", currency: "BRL" });
    const usd = Money.from({ amount: "10.00", currency: "USD" });
    expect(() => brl.add(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl.subtract(usd)).toThrow(CurrencyMismatchError);
  });

  it("negate() flips the sign", () => {
    const money = Money.from({ amount: "25.00", currency: "BRL" });
    expect(money.negate().toString()).toBe("-25.00");
  });

  it("isLessThan compares same-currency amounts", () => {
    const a = Money.from({ amount: "10.00", currency: "BRL" });
    const b = Money.from({ amount: "20.00", currency: "BRL" });
    expect(a.isLessThan(b)).toBe(true);
    expect(b.isLessThan(a)).toBe(false);
  });

  it("equals compares amount and currency", () => {
    const a = Money.from({ amount: "10.00", currency: "BRL" });
    const b = Money.from({ amount: "10.00", currency: "BRL" });
    expect(a.equals(b)).toBe(true);
  });

  it("rehydrate() accepts values coming back from the database, including negative (ledger deltas)", () => {
    const rehydrated = Money.rehydrate({ amount: "-25.0000", currency: "BRL" });
    expect(rehydrated.toString()).toBe("-25.00");
  });

  it("toJSON() serializes as a stable decimal string", () => {
    const money = Money.from({ amount: "25.00", currency: "BRL" });
    expect(money.toJSON()).toEqual({ amount: "25.00", currency: "BRL" });
  });
});
