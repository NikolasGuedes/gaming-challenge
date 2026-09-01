import { Decimal } from "decimal.js";
import { CurrencyMismatchError, InvalidMoneyError } from "./money.errors";

const DECIMAL_STRING_PATTERN = /^-?\d+(\.\d{1,2})?$/;

export class Money {
  private constructor(
    private readonly _amount: Decimal,
    private readonly _currency: string,
  ) {}

  static from(input: { amount: string; currency: string }): Money {
    const { amount, currency } = input;
    if (typeof amount !== "string" || amount.trim() === "") {
      throw new InvalidMoneyError("amount must be a non-empty string", input);
    }
    if (!DECIMAL_STRING_PATTERN.test(amount)) {
      throw new InvalidMoneyError(
        "amount must be a plain decimal string with at most 2 decimal places (no scientific notation, NaN or Infinity)",
        input,
      );
    }
    if (amount.startsWith("-")) {
      throw new InvalidMoneyError("amount must not be negative at entry", input);
    }
    if (!currency || currency.length !== 3) {
      throw new InvalidMoneyError("currency must be a 3-letter ISO-4217 code", input);
    }
    return new Money(new Decimal(amount).toDecimalPlaces(2), currency);
  }

  static zero(currency: string): Money {
    return new Money(new Decimal(0).toDecimalPlaces(2), currency);
  }

  /** Rebuilds a Money from a trusted source (DB row, ledger delta) — allows negative values. */
  static rehydrate(input: { amount: string; currency: string }): Money {
    const parsed = new Decimal(input.amount);
    if (!parsed.isFinite()) {
      throw new InvalidMoneyError("rehydrated amount must be finite", input);
    }
    return new Money(parsed.toDecimalPlaces(2), input.currency);
  }

  get amount(): Decimal {
    return this._amount;
  }

  get currency(): string {
    return this._currency;
  }

  private assertSameCurrency(other: Money): void {
    if (this._currency !== other._currency) {
      throw new CurrencyMismatchError(this._currency, other._currency);
    }
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this._amount.add(other._amount), this._currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this._amount.sub(other._amount), this._currency);
  }

  negate(): Money {
    return new Money(this._amount.neg(), this._currency);
  }

  isZero(): boolean {
    return this._amount.isZero();
  }

  isPositive(): boolean {
    return this._amount.isPositive() && !this._amount.isZero();
  }

  isNegative(): boolean {
    return this._amount.isNegative();
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this._amount.lessThan(other._amount);
  }

  equals(other: Money): boolean {
    return this._currency === other._currency && this._amount.equals(other._amount);
  }

  toJSON(): { amount: string; currency: string } {
    return { amount: this._amount.toFixed(2), currency: this._currency };
  }

  toString(): string {
    return this._amount.toFixed(2);
  }
}
