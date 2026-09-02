# Wagering Processor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Distributed Wagering Processor — a financial service that
processes BET/WIN/LOSS/REFUND/ROLLBACK transactions from HTTP and SQS,
correct under concurrency, persistently idempotent, with an auditable
ledger.

**Architecture:** Domain classes (Money, Wallet, WagerTransaction,
WalletLedgerEntry) are plain TypeScript with no NestJS/ORM dependency,
mapped to MikroORM entities via dedicated mappers. HTTP controller and SQS
consumer are thin adapters that both call the same `ProcessWagerUseCase`.
Concurrency is handled with `SELECT ... FOR UPDATE` row locks per wallet.
Reliable delivery uses the transactional outbox (write in the same SQL
transaction as the ledger, publish later via polling + `SKIP LOCKED`) and
transactional inbox (dedup by `(consumerName, messageId)`) patterns.

**Tech Stack:** Bun 1.x (runtime, package manager, test runner), NestJS,
MikroORM + `@mikro-orm/postgresql`, PostgreSQL, AWS SQS via LocalStack
(`@aws-sdk/client-sqs`), `decimal.js`, Testcontainers for integration
tests.

**Spec:** `docs/superpowers/specs/2026-09-01-wagering-processor-architecture-design.md`

## Global Constraints

- Money is **never** `number`/`float`/`double` anywhere in the flow —
  `Decimal` internally, decimal string (2 places) at the HTTP boundary.
- Domain classes (`Money`, `Wallet`, `WagerTransaction`, `WalletLedgerEntry`)
  have **zero** imports from `@nestjs/*` or `@mikro-orm/*`.
- Every financial write (wallet update + ledger insert + outbox insert +
  inbox insert when applicable) happens in **one** SQL transaction; outbox
  rows are only visible after that transaction commits.
- HTTP controller and SQS consumer both call the same
  `ProcessWagerUseCase` — no duplicated business logic per entry point.
- Idempotency is persistent (Postgres unique constraints), never in-memory.
- Uniqueness, immutability (ledger), and non-negativity (balance) are
  enforced **in the database schema**, not only in application code.
- `REFUND` may only reference a `BET`; `ROLLBACK` may reference `BET`,
  `WIN`, or `REFUND`.
- No load testing, no metrics stack (Prometheus/Grafana) — structured
  JSON logs + SQL queries for observability, per the reduced-scope
  decision in the spec (§0).
- Package manager and test runner: `bun`, not `npm`/`vitest`. Test files
  end in `.spec.ts` (unit, colocated) or live under `test/` for e2e/
  integration, matching bun's filename convention.

---

## File Structure

```
src/
  shared-kernel/
    money.ts
    money.errors.ts
    failure-code.ts

  wallet/
    domain/
      wallet.ts
      wallet-ledger-entry.ts
    application/
      ports/
        wallet.repository.ts
      use-cases/
        create-wallet.use-case.ts
        reconcile-wallet.use-case.ts
    infrastructure/
      persistence/
        entities/wallet.entity.ts
        entities/wallet-ledger-entry.entity.ts
        mappers/wallet.mapper.ts
        repositories/wallet.repository.ts
      http/
        wallet.controller.ts
        dto/create-wallet.dto.ts
        dto/wallet-response.dto.ts
    wallet.module.ts

  wagering/
    domain/
      wager-transaction.ts
    application/
      ports/
        wager-transaction.repository.ts
        idempotency-key.repository.ts
      use-cases/
        process-wager.use-case.ts
    infrastructure/
      persistence/
        entities/wager-transaction.entity.ts
        entities/idempotency-key.entity.ts
        mappers/wager-transaction.mapper.ts
        repositories/wager-transaction.repository.ts
        repositories/idempotency-key.repository.ts
      http/
        wagering.controller.ts
        idempotency.service.ts
        dto/submit-wager.dto.ts
        dto/wager-response.dto.ts
    wagering.module.ts

  messaging/
    application/
      ports/
        inbox.repository.ts
        outbox.repository.ts
        message-publisher.port.ts
    infrastructure/
      persistence/
        entities/inbox-message.entity.ts
        entities/outbox-message.entity.ts
        repositories/inbox.repository.ts
        repositories/outbox.repository.ts
      sqs/
        sqs-client.provider.ts
        sqs-message-publisher.ts
        wager-transaction.consumer.ts
        outbox-publisher.worker.ts
        bootstrap-queues.ts
    messaging.module.ts

  health/
    health.controller.ts
    health.module.ts

  migrations/
    Migration20260901000000.ts

  mikro-orm.config.ts
  app.module.ts
  main.ts

test/
  support/
    testcontainers-env.ts
  wallet-ledger-atomicity.spec.ts
  idempotency-replay.spec.ts
  concurrency-hot-wallet.spec.ts
  outbox-skip-locked.spec.ts
  inbox-redelivery.spec.ts
  pending-reference.spec.ts
  reconciliation.spec.ts
```

---

## Task 1: Money value object (shared kernel)

**Files:**
- Create: `src/shared-kernel/money.errors.ts`
- Create: `src/shared-kernel/money.ts`
- Test: `src/shared-kernel/money.spec.ts`

**Interfaces:**
- Consumes: `decimal.js` (`Decimal` class), already installed.
- Produces:
  - `class InvalidMoneyError extends Error` (constructor: `(reason: string, input: unknown)`)
  - `class CurrencyMismatchError extends Error` (constructor: `(left: string, right: string)`)
  - `class Money`:
    - `static from(input: { amount: string; currency: string }): Money`
    - `static zero(currency: string): Money`
    - `static rehydrate(input: { amount: string; currency: string }): Money`
    - `get amount(): Decimal`
    - `get currency(): string`
    - `add(other: Money): Money`
    - `subtract(other: Money): Money`
    - `negate(): Money`
    - `isZero(): boolean`
    - `isPositive(): boolean`
    - `isNegative(): boolean`
    - `isLessThan(other: Money): boolean`
    - `equals(other: Money): boolean`
    - `toJSON(): { amount: string; currency: string }`
    - `toString(): string`

- [ ] **Step 1: Write the failing test**

```typescript
// src/shared-kernel/money.spec.ts
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bun test src/shared-kernel/money.spec.ts`
Expected: FAIL with `Cannot find module './money'` (or `./money.errors`).

- [ ] **Step 3: Write the errors module**

```typescript
// src/shared-kernel/money.errors.ts
export class InvalidMoneyError extends Error {
  constructor(reason: string, input: unknown) {
    super(`Invalid money value: ${reason} (received ${JSON.stringify(input)})`);
    this.name = "InvalidMoneyError";
  }
}

export class CurrencyMismatchError extends Error {
  constructor(left: string, right: string) {
    super(`Currency mismatch: ${left} vs ${right}`);
    this.name = "CurrencyMismatchError";
  }
}
```

- [ ] **Step 4: Write the Money implementation**

```typescript
// src/shared-kernel/money.ts
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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `bun test src/shared-kernel/money.spec.ts`
Expected: PASS (14 tests).

- [ ] **Step 6: Commit**

```bash
git add src/shared-kernel/money.ts src/shared-kernel/money.errors.ts src/shared-kernel/money.spec.ts
git commit -m "feat: add Money value object with decimal.js precision"
```

---

## Task 2: FailureCode enum + Wallet aggregate + WalletLedgerEntry

**Files:**
- Create: `src/shared-kernel/failure-code.ts`
- Create: `src/wallet/domain/wallet-ledger-entry.ts`
- Create: `src/wallet/domain/wallet.ts`
- Test: `src/wallet/domain/wallet.spec.ts`

**Interfaces:**
- Consumes: `Money` from `src/shared-kernel/money.ts` (Task 1).
- Produces:
  - `enum FailureCode` with values `INSUFFICIENT_FUNDS`, `CURRENCY_MISMATCH`,
    `REFERENCE_NOT_FOUND`, `INVALID_REFERENCE_KIND`,
    `REFERENCE_ALREADY_REVERSED`, `IDEMPOTENCY_KEY_CONFLICT`.
  - `type LedgerDirection = "DEBIT" | "CREDIT"`
  - `class WalletLedgerEntry`:
    - `static create(input: { id: string; walletId: string; transactionId: string; direction: LedgerDirection; amount: Money; balanceBefore: Money; balanceAfter: Money; createdAt?: Date }): WalletLedgerEntry`
    - `static rehydrate(input: same fields, all required, id/createdAt included): WalletLedgerEntry`
    - readonly getters: `id, walletId, transactionId, direction, amount, balanceBefore, balanceAfter, createdAt`
  - `type WalletApplyResult = { status: "PROCESSED"; wallet: Wallet; entry: WalletLedgerEntry } | { status: "REJECTED"; failureCode: FailureCode }`
  - `class Wallet`:
    - `static open(input: { id: string; playerId: string; currency: string; initialBalance: Money; openingTransactionId: string }): { wallet: Wallet; openingEntry: WalletLedgerEntry | null }`
    - `static rehydrate(input: { id: string; playerId: string; currency: string; balance: Money; version: number }): Wallet`
    - readonly getters: `id, playerId, currency, balance, version`
    - `debit(input: { amount: Money; transactionId: string }): WalletApplyResult`
    - `credit(input: { amount: Money; transactionId: string }): WalletApplyResult`

- [ ] **Step 1: Write the failing test**

```typescript
// src/wallet/domain/wallet.spec.ts
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bun test src/wallet/domain/wallet.spec.ts`
Expected: FAIL — modules don't exist yet.

- [ ] **Step 3: Write FailureCode**

```typescript
// src/shared-kernel/failure-code.ts
export enum FailureCode {
  INSUFFICIENT_FUNDS = "INSUFFICIENT_FUNDS",
  CURRENCY_MISMATCH = "CURRENCY_MISMATCH",
  REFERENCE_NOT_FOUND = "REFERENCE_NOT_FOUND",
  INVALID_REFERENCE_KIND = "INVALID_REFERENCE_KIND",
  REFERENCE_ALREADY_REVERSED = "REFERENCE_ALREADY_REVERSED",
  IDEMPOTENCY_KEY_CONFLICT = "IDEMPOTENCY_KEY_CONFLICT",
}
```

- [ ] **Step 4: Write WalletLedgerEntry**

```typescript
// src/wallet/domain/wallet-ledger-entry.ts
import { Money } from "../../shared-kernel/money";

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
```

- [ ] **Step 5: Write Wallet**

```typescript
// src/wallet/domain/wallet.ts
import { Money } from "../../shared-kernel/money";
import { FailureCode } from "../../shared-kernel/failure-code";
import { WalletLedgerEntry } from "./wallet-ledger-entry";

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
```

- [ ] **Step 6: Run test to verify it passes**

Run: `bun test src/wallet/domain/wallet.spec.ts`
Expected: PASS (7 tests).

- [ ] **Step 7: Commit**

```bash
git add src/shared-kernel/failure-code.ts src/wallet/domain/wallet.ts src/wallet/domain/wallet-ledger-entry.ts src/wallet/domain/wallet.spec.ts
git commit -m "feat: add Wallet aggregate and WalletLedgerEntry domain classes"
```

---

## Task 3: WagerTransaction domain + canonical payload hash

**Files:**
- Create: `src/wagering/domain/payload-hash.ts`
- Create: `src/wagering/domain/wager-transaction.ts`
- Test: `src/wagering/domain/payload-hash.spec.ts`
- Test: `src/wagering/domain/wager-transaction.spec.ts`

**Interfaces:**
- Consumes: `Money` (Task 1), `FailureCode` (Task 2).
- Produces:
  - `computePayloadHash(businessFields: Record<string, unknown>): string`
  - `type WagerKind = "BET" | "WIN" | "LOSS" | "REFUND" | "ROLLBACK" | "OPENING"`
  - `isExternallySubmittableKind(kind: WagerKind): boolean` (false only for `"OPENING"`)
  - `type WagerStatus = "PENDING" | "PENDING_REFERENCE" | "PROCESSED" | "REJECTED" | "FAILED"`
  - `validateReferenceKind(kind: WagerKind, referencedKind: WagerKind): FailureCode | null`
  - `class WagerTransaction`:
    - `static create(input: { id: string; walletId: string; externalTransactionId: string; providerId: string; idempotencyKey: string; payloadHash: string; kind: WagerKind; amount: Money; referenceExternalTransactionId: string | null }): WagerTransaction` (`amount` is the transaction's own face value — for `REFUND`/`ROLLBACK` this is the amount being reversed, copied from the referenced transaction, never taken from client input)
    - `static rehydrate(input: full fields incl. status, failureCode, resultBalance, createdAt, updatedAt): WagerTransaction`
    - readonly getters for every field
    - `markProcessed(resultBalance: Money): WagerTransaction`
    - `markRejected(failureCode: FailureCode): WagerTransaction`
    - `markPendingReference(): WagerTransaction`

- [ ] **Step 1: Write the failing tests**

```typescript
// src/wagering/domain/payload-hash.spec.ts
import { describe, expect, it } from "bun:test";
import { computePayloadHash } from "./payload-hash";

describe("computePayloadHash", () => {
  it("is stable regardless of key order", () => {
    const a = computePayloadHash({ amount: "25.00", currency: "BRL", walletId: "w1" });
    const b = computePayloadHash({ walletId: "w1", currency: "BRL", amount: "25.00" });
    expect(a).toBe(b);
  });

  it("differs when a value differs", () => {
    const a = computePayloadHash({ amount: "25.00", currency: "BRL" });
    const b = computePayloadHash({ amount: "25.01", currency: "BRL" });
    expect(a).not.toBe(b);
  });

  it("produces a 64-char hex sha256 digest", () => {
    const hash = computePayloadHash({ amount: "25.00" });
    expect(hash).toMatch(/^[0-9a-f]{64}$/);
  });
});
```

```typescript
// src/wagering/domain/wager-transaction.spec.ts
import { describe, expect, it } from "bun:test";
import { Money } from "../../shared-kernel/money";
import { FailureCode } from "../../shared-kernel/failure-code";
import {
  isExternallySubmittableKind,
  validateReferenceKind,
  WagerTransaction,
} from "./wager-transaction";

describe("isExternallySubmittableKind", () => {
  it("rejects OPENING, accepts the rest", () => {
    expect(isExternallySubmittableKind("OPENING")).toBe(false);
    expect(isExternallySubmittableKind("BET")).toBe(true);
    expect(isExternallySubmittableKind("REFUND")).toBe(true);
  });
});

describe("validateReferenceKind", () => {
  it("REFUND only accepts a BET reference", () => {
    expect(validateReferenceKind("REFUND", "BET")).toBeNull();
    expect(validateReferenceKind("REFUND", "WIN")).toBe(FailureCode.INVALID_REFERENCE_KIND);
    expect(validateReferenceKind("REFUND", "ROLLBACK")).toBe(FailureCode.INVALID_REFERENCE_KIND);
  });

  it("ROLLBACK accepts BET, WIN or REFUND references", () => {
    expect(validateReferenceKind("ROLLBACK", "BET")).toBeNull();
    expect(validateReferenceKind("ROLLBACK", "WIN")).toBeNull();
    expect(validateReferenceKind("ROLLBACK", "REFUND")).toBeNull();
    expect(validateReferenceKind("ROLLBACK", "LOSS")).toBe(FailureCode.INVALID_REFERENCE_KIND);
  });

  it("non-reversal kinds have nothing to validate", () => {
    expect(validateReferenceKind("BET", "BET")).toBeNull();
  });
});

describe("WagerTransaction", () => {
  const baseInput = {
    id: "tx-1",
    walletId: "wallet-1",
    externalTransactionId: "ext-1",
    providerId: "provider-a",
    idempotencyKey: "idem-1",
    payloadHash: "hash-1",
    kind: "BET" as const,
    amount: Money.from({ amount: "25.00", currency: "BRL" }),
    referenceExternalTransactionId: null,
  };

  it("create() starts in PENDING status", () => {
    const tx = WagerTransaction.create(baseInput);
    expect(tx.status).toBe("PENDING");
    expect(tx.failureCode).toBeNull();
    expect(tx.resultBalance).toBeNull();
  });

  it("markProcessed() returns a new PROCESSED instance carrying the result balance", () => {
    const tx = WagerTransaction.create(baseInput);
    const balance = Money.from({ amount: "20.00", currency: "BRL" });
    const processed = tx.markProcessed(balance);
    expect(processed.status).toBe("PROCESSED");
    expect(processed.resultBalance?.toString()).toBe("20.00");
    expect(tx.status).toBe("PENDING"); // original untouched
  });

  it("markRejected() sets the failure code", () => {
    const tx = WagerTransaction.create(baseInput);
    const rejected = tx.markRejected(FailureCode.INSUFFICIENT_FUNDS);
    expect(rejected.status).toBe("REJECTED");
    expect(rejected.failureCode).toBe(FailureCode.INSUFFICIENT_FUNDS);
  });

  it("markPendingReference() sets PENDING_REFERENCE", () => {
    const tx = WagerTransaction.create({
      ...baseInput,
      kind: "REFUND",
      referenceExternalTransactionId: "ext-0",
    });
    const pending = tx.markPendingReference();
    expect(pending.status).toBe("PENDING_REFERENCE");
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `bun test src/wagering/domain/`
Expected: FAIL — modules don't exist yet.

- [ ] **Step 3: Write payload-hash.ts**

```typescript
// src/wagering/domain/payload-hash.ts
import { createHash } from "node:crypto";

function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(canonicalize);
  }
  if (value !== null && typeof value === "object") {
    const sorted: Record<string, unknown> = {};
    for (const key of Object.keys(value as Record<string, unknown>).sort()) {
      sorted[key] = canonicalize((value as Record<string, unknown>)[key]);
    }
    return sorted;
  }
  return value;
}

/** Canonical JSON (sorted keys) sha256 hash of the business-relevant subset of a payload. */
export function computePayloadHash(businessFields: Record<string, unknown>): string {
  const canonicalJson = JSON.stringify(canonicalize(businessFields));
  return createHash("sha256").update(canonicalJson).digest("hex");
}
```

- [ ] **Step 4: Write wager-transaction.ts**

```typescript
// src/wagering/domain/wager-transaction.ts
import { Money } from "../../shared-kernel/money";
import { FailureCode } from "../../shared-kernel/failure-code";

export type WagerKind = "BET" | "WIN" | "LOSS" | "REFUND" | "ROLLBACK" | "OPENING";

/** OPENING is created only by CreateWalletUseCase — never accepted from HTTP or SQS. */
export function isExternallySubmittableKind(kind: WagerKind): boolean {
  return kind !== "OPENING";
}

export type WagerStatus = "PENDING" | "PENDING_REFERENCE" | "PROCESSED" | "REJECTED" | "FAILED";

const REFUND_ALLOWED_REFERENCE_KINDS: readonly WagerKind[] = ["BET"];
const ROLLBACK_ALLOWED_REFERENCE_KINDS: readonly WagerKind[] = ["BET", "WIN", "REFUND"];

export function validateReferenceKind(kind: WagerKind, referencedKind: WagerKind): FailureCode | null {
  if (kind === "REFUND") {
    return REFUND_ALLOWED_REFERENCE_KINDS.includes(referencedKind) ? null : FailureCode.INVALID_REFERENCE_KIND;
  }
  if (kind === "ROLLBACK") {
    return ROLLBACK_ALLOWED_REFERENCE_KINDS.includes(referencedKind) ? null : FailureCode.INVALID_REFERENCE_KIND;
  }
  return null;
}

export class WagerTransaction {
  private constructor(
    readonly id: string,
    readonly walletId: string,
    readonly externalTransactionId: string,
    readonly providerId: string,
    readonly idempotencyKey: string,
    readonly payloadHash: string,
    readonly kind: WagerKind,
    readonly amount: Money,
    readonly referenceExternalTransactionId: string | null,
    readonly status: WagerStatus,
    readonly failureCode: FailureCode | null,
    readonly resultBalance: Money | null,
    readonly createdAt: Date,
    readonly updatedAt: Date,
  ) {}

  static create(input: {
    id: string;
    walletId: string;
    externalTransactionId: string;
    providerId: string;
    idempotencyKey: string;
    payloadHash: string;
    kind: WagerKind;
    amount: Money;
    referenceExternalTransactionId: string | null;
  }): WagerTransaction {
    const now = new Date();
    return new WagerTransaction(
      input.id,
      input.walletId,
      input.externalTransactionId,
      input.providerId,
      input.idempotencyKey,
      input.payloadHash,
      input.kind,
      input.amount,
      input.referenceExternalTransactionId,
      "PENDING",
      null,
      null,
      now,
      now,
    );
  }

  static rehydrate(input: {
    id: string;
    walletId: string;
    externalTransactionId: string;
    providerId: string;
    idempotencyKey: string;
    payloadHash: string;
    kind: WagerKind;
    amount: Money;
    referenceExternalTransactionId: string | null;
    status: WagerStatus;
    failureCode: FailureCode | null;
    resultBalance: Money | null;
    createdAt: Date;
    updatedAt: Date;
  }): WagerTransaction {
    return new WagerTransaction(
      input.id,
      input.walletId,
      input.externalTransactionId,
      input.providerId,
      input.idempotencyKey,
      input.payloadHash,
      input.kind,
      input.amount,
      input.referenceExternalTransactionId,
      input.status,
      input.failureCode,
      input.resultBalance,
      input.createdAt,
      input.updatedAt,
    );
  }

  private with(patch: Partial<{ status: WagerStatus; failureCode: FailureCode | null; resultBalance: Money | null }>): WagerTransaction {
    return new WagerTransaction(
      this.id,
      this.walletId,
      this.externalTransactionId,
      this.providerId,
      this.idempotencyKey,
      this.payloadHash,
      this.kind,
      this.amount,
      this.referenceExternalTransactionId,
      patch.status ?? this.status,
      patch.failureCode !== undefined ? patch.failureCode : this.failureCode,
      patch.resultBalance !== undefined ? patch.resultBalance : this.resultBalance,
      this.createdAt,
      new Date(),
    );
  }

  markProcessed(resultBalance: Money): WagerTransaction {
    return this.with({ status: "PROCESSED", failureCode: null, resultBalance });
  }

  markRejected(failureCode: FailureCode): WagerTransaction {
    return this.with({ status: "REJECTED", failureCode, resultBalance: null });
  }

  markPendingReference(): WagerTransaction {
    return this.with({ status: "PENDING_REFERENCE" });
  }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `bun test src/wagering/domain/`
Expected: PASS (11 tests).

- [ ] **Step 6: Commit**

```bash
git add src/wagering/domain/
git commit -m "feat: add WagerTransaction domain and canonical payload hash"
```

---

## Task 4: MikroORM bootstrap and NestJS wiring

**Files:**
- Create: `mikro-orm.config.ts`
- Create: `src/app.module.ts`
- Create: `src/main.ts` (replace the Nest-generated placeholder)
- Modify: `package.json` (add `"mikro-orm"` script — already present from setup)

**Interfaces:**
- Consumes: `DATABASE_URL` env var (already in `.env`/`.env.example`).
- Produces: a MikroORM config resolvable by both the `mikro-orm` CLI and
  `MikroOrmModule.forRoot()` in Nest; later tasks register their entities
  and modules into `AppModule`'s `imports` array.

- [ ] **Step 1: Write the MikroORM config**

```typescript
// mikro-orm.config.ts
import { defineConfig } from "@mikro-orm/postgresql";

export default defineConfig({
  clientUrl: process.env.DATABASE_URL,
  entities: ["dist/**/*.entity.js"],
  entitiesTs: ["src/**/*.entity.ts"],
  migrations: {
    path: "dist/migrations",
    pathTs: "src/migrations",
  },
  debug: process.env.NODE_ENV !== "production",
});
```

- [ ] **Step 2: Write the root AppModule (health only for now — other modules register in later tasks)**

```typescript
// src/app.module.ts
import { Module } from "@nestjs/common";
import { MikroOrmModule } from "@mikro-orm/nestjs";
import { ConfigModule } from "@nestjs/config";
import mikroOrmConfig from "../mikro-orm.config";

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    MikroOrmModule.forRoot(mikroOrmConfig),
  ],
})
export class AppModule {}
```

- [ ] **Step 3: Write main.ts**

```typescript
// src/main.ts
import "reflect-metadata";
import { NestFactory } from "@nestjs/core";
import { ValidationPipe } from "@nestjs/common";
import { AppModule } from "./app.module";

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  app.enableShutdownHooks();
  const port = process.env.PORT ?? 3000;
  await app.listen(port);
}

bootstrap();
```

- [ ] **Step 4: Remove the Nest-generated placeholder files no longer used**

```bash
rm -f src/app.controller.ts src/app.service.ts src/app.controller.spec.ts
```

- [ ] **Step 5: Verify the config resolves (no DB write, just config load)**

Run: `docker compose up -d postgres`
Run: `bunx mikro-orm debug`
Expected: prints the resolved configuration (clientUrl, entities paths) without throwing.

- [ ] **Step 6: Verify the app still boots**

Run: `bun run build && bun run start`
Expected: Nest boots, logs `Nest application successfully started`, no
controller routes yet (health added in Task 17). Stop with Ctrl+C.

- [ ] **Step 7: Commit**

```bash
git add mikro-orm.config.ts src/app.module.ts src/main.ts
git rm src/app.controller.ts src/app.service.ts src/app.controller.spec.ts
git commit -m "chore: wire MikroORM into NestJS, remove scaffold placeholder"
```

---

## Task 5: Initial schema migration (all constraints in the database)

**Files:**
- Create: `src/migrations/Migration20260901000000.ts`

**Interfaces:**
- Consumes: none (raw SQL migration, runs against the Postgres started by
  `docker-compose.yml` or by Testcontainers in later integration tests).
- Produces: tables `wallets`, `wager_transactions`, `wallet_ledger_entries`,
  `idempotency_keys`, `inbox_messages`, `outbox_messages`, with every
  constraint listed in the spec (§11) enforced by the schema itself —
  later tasks' entities (Task 6-8) map onto these exact column names.

- [ ] **Step 1: Write the migration**

```typescript
// src/migrations/Migration20260901000000.ts
import { Migration } from "@mikro-orm/migrations";

export class Migration20260901000000 extends Migration {
  async up(): Promise<void> {
    this.addSql(`
      create table "wallets" (
        "id" uuid not null primary key,
        "player_id" text not null,
        "currency" char(3) not null,
        "balance" numeric(19,4) not null default 0,
        "version" int not null default 1,
        "created_at" timestamptz not null default now(),
        "updated_at" timestamptz not null default now(),
        constraint "wallets_balance_non_negative" check ("balance" >= 0),
        constraint "wallets_player_currency_unique" unique ("player_id", "currency")
      );
    `);

    this.addSql(`
      create table "wager_transactions" (
        "id" uuid not null primary key,
        "wallet_id" uuid not null references "wallets" ("id"),
        "external_transaction_id" text not null,
        "provider_id" text not null,
        "idempotency_key" text not null,
        "payload_hash" text not null,
        "kind" text not null check ("kind" in ('BET','WIN','LOSS','REFUND','ROLLBACK','OPENING')),
        "amount" numeric(19,4) not null,
        "currency" char(3) not null,
        "reference_external_transaction_id" text null,
        "status" text not null check ("status" in ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        "failure_code" text null,
        "result_balance_amount" numeric(19,4) null,
        "result_balance_currency" char(3) null,
        "created_at" timestamptz not null default now(),
        "updated_at" timestamptz not null default now(),
        constraint "wager_transactions_idempotency_key_unique" unique ("idempotency_key"),
        constraint "wager_transactions_provider_external_unique" unique ("provider_id", "external_transaction_id")
      );
    `);

    this.addSql(`
      create table "wallet_ledger_entries" (
        "id" uuid not null primary key,
        "wallet_id" uuid not null references "wallets" ("id"),
        "transaction_id" uuid not null references "wager_transactions" ("id"),
        "direction" text not null check ("direction" in ('DEBIT','CREDIT')),
        "amount" numeric(19,4) not null,
        "currency" char(3) not null,
        "balance_before" numeric(19,4) not null,
        "balance_after" numeric(19,4) not null,
        "created_at" timestamptz not null default now(),
        constraint "wallet_ledger_entries_wallet_transaction_unique" unique ("wallet_id", "transaction_id")
      );
    `);

    // Ledger is append-only: no UPDATE or DELETE, enforced at the schema level.
    this.addSql(`
      create function "prevent_ledger_mutation"() returns trigger as $$
      begin
        raise exception 'wallet_ledger_entries is append-only: % is not allowed', TG_OP;
      end;
      $$ language plpgsql;
    `);
    this.addSql(`
      create trigger "wallet_ledger_entries_no_mutation"
      before update or delete on "wallet_ledger_entries"
      for each row execute function "prevent_ledger_mutation"();
    `);

    this.addSql(`
      create table "idempotency_keys" (
        "key" text not null primary key,
        "payload_hash" text not null,
        "response" jsonb not null,
        "created_at" timestamptz not null default now()
      );
    `);

    this.addSql(`
      create table "inbox_messages" (
        "consumer_name" text not null,
        "message_id" text not null,
        "processed_at" timestamptz not null default now(),
        primary key ("consumer_name", "message_id")
      );
    `);

    this.addSql(`
      create table "outbox_messages" (
        "id" uuid not null primary key,
        "aggregate_id" uuid not null,
        "event_type" text not null,
        "payload" jsonb not null,
        "occurred_at" timestamptz not null,
        "attempts" int not null default 0,
        "next_attempt_at" timestamptz not null default now(),
        "published_at" timestamptz null
      );
    `);
    this.addSql(`
      create index "outbox_messages_publish_lookup"
      on "outbox_messages" ("published_at", "next_attempt_at");
    `);
  }

  async down(): Promise<void> {
    this.addSql(`drop table if exists "outbox_messages" cascade;`);
    this.addSql(`drop table if exists "inbox_messages" cascade;`);
    this.addSql(`drop table if exists "idempotency_keys" cascade;`);
    this.addSql(`drop trigger if exists "wallet_ledger_entries_no_mutation" on "wallet_ledger_entries";`);
    this.addSql(`drop function if exists "prevent_ledger_mutation";`);
    this.addSql(`drop table if exists "wallet_ledger_entries" cascade;`);
    this.addSql(`drop table if exists "wager_transactions" cascade;`);
    this.addSql(`drop table if exists "wallets" cascade;`);
  }
}
```

- [ ] **Step 2: Run the migration against the local Postgres**

Run: `docker compose up -d postgres`
Run: `bunx mikro-orm migration:up`
Expected: prints the migration name as applied, exits 0.

- [ ] **Step 3: Verify the constraints actually exist**

Run:
```bash
docker compose exec postgres psql -U wagering -d wagering -c "\d wallets"
docker compose exec postgres psql -U wagering -d wagering -c "\d wallet_ledger_entries"
```
Expected: `\d wallets` shows the `wallets_balance_non_negative` check and the
`wallets_player_currency_unique` unique constraint; `\d wallet_ledger_entries`
lists the `wallet_ledger_entries_no_mutation` trigger.

- [ ] **Step 4: Verify ledger immutability manually**

Run:
```bash
docker compose exec postgres psql -U wagering -d wagering -c "insert into wallets (id, player_id, currency) values (gen_random_uuid(), 'p1', 'BRL');"
```
(If `gen_random_uuid()` is unavailable, use a literal uuid string instead —
the app never relies on this function, ids are generated in TypeScript.)
Then attempt an `update`/`delete` on `wallet_ledger_entries` and confirm
Postgres raises the `append-only` exception.

- [ ] **Step 5: Commit**

```bash
git add src/migrations/Migration20260901000000.ts
git commit -m "feat: add initial schema migration with all invariants enforced in Postgres"
```

---

## Task 6: Wallet persistence (entities, mappers, repository, FOR UPDATE lock) + Testcontainers helper

**Files:**
- Create: `test/support/testcontainers-env.ts`
- Create: `src/wallet/infrastructure/persistence/entities/wallet.entity.ts`
- Create: `src/wallet/infrastructure/persistence/entities/wallet-ledger-entry.entity.ts`
- Create: `src/wallet/infrastructure/persistence/mappers/wallet.mapper.ts`
- Create: `src/wallet/application/ports/wallet.repository.ts`
- Create: `src/wallet/infrastructure/persistence/repositories/wallet.repository.ts`
- Test: `test/wallet-repository-lock.spec.ts`

**Interfaces:**
- Consumes: `Wallet`, `WalletLedgerEntry` (Task 2), `mikro-orm.config.ts` (Task 4), schema from Task 5.
- Produces:
  - `startTestDatabase(): Promise<{ container, orm }>` / `stopTestDatabase(db): Promise<void>` — reused by every later integration test.
  - `class WalletEntity`, `class WalletLedgerEntryEntity` (MikroORM).
  - `WalletMapper.toDomain(entity): Wallet`, `WalletMapper.toEntity(wallet, existing?): WalletEntity`.
  - `WalletLedgerEntryMapper.toDomain(entity): WalletLedgerEntry`, `WalletLedgerEntryMapper.toEntity(entry): WalletLedgerEntryEntity`.
  - `interface WalletRepository` with `findById`, `findByIdForUpdate`, `findByPlayerAndCurrency`, `save`, `appendLedgerEntry`, `listLedgerEntries`, `sumLedgerEntries` (the last one is added and used starting in Task 13).
  - `class MikroOrmWalletRepository implements WalletRepository` (constructor takes `EntityManager`).

- [ ] **Step 1: Write the Testcontainers helper**

```typescript
// test/support/testcontainers-env.ts
import { PostgreSqlContainer, StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { MikroORM } from "@mikro-orm/postgresql";
import config from "../../mikro-orm.config";

export interface TestDatabase {
  container: StartedPostgreSqlContainer;
  orm: MikroORM;
}

export async function startTestDatabase(): Promise<TestDatabase> {
  const container = await new PostgreSqlContainer("postgres:16-alpine")
    .withDatabase("wagering_test")
    .withUsername("wagering")
    .withPassword("wagering")
    .start();

  const orm = await MikroORM.init({
    ...config,
    clientUrl: container.getConnectionUri(),
  });
  await orm.getMigrator().up();
  return { container, orm };
}

export async function stopTestDatabase(db: TestDatabase): Promise<void> {
  await db.orm.close(true);
  await db.container.stop();
}
```

- [ ] **Step 2: Write the entities**

```typescript
// src/wallet/infrastructure/persistence/entities/wallet.entity.ts
import { Entity, PrimaryKey, Property } from "@mikro-orm/postgresql";

@Entity({ tableName: "wallets" })
export class WalletEntity {
  @PrimaryKey({ type: "uuid" })
  id!: string;

  @Property()
  playerId!: string;

  @Property({ length: 3 })
  currency!: string;

  @Property({ type: "decimal", precision: 19, scale: 4 })
  balance!: string;

  @Property()
  version!: number;

  @Property({ onCreate: () => new Date() })
  createdAt: Date = new Date();

  @Property({ onCreate: () => new Date(), onUpdate: () => new Date() })
  updatedAt: Date = new Date();
}
```

```typescript
// src/wallet/infrastructure/persistence/entities/wallet-ledger-entry.entity.ts
import { Entity, PrimaryKey, Property } from "@mikro-orm/postgresql";

@Entity({ tableName: "wallet_ledger_entries" })
export class WalletLedgerEntryEntity {
  @PrimaryKey({ type: "uuid" })
  id!: string;

  @Property()
  walletId!: string;

  @Property()
  transactionId!: string;

  @Property()
  direction!: "DEBIT" | "CREDIT";

  @Property({ type: "decimal", precision: 19, scale: 4 })
  amount!: string;

  @Property({ length: 3 })
  currency!: string;

  @Property({ type: "decimal", precision: 19, scale: 4 })
  balanceBefore!: string;

  @Property({ type: "decimal", precision: 19, scale: 4 })
  balanceAfter!: string;

  @Property({ onCreate: () => new Date() })
  createdAt: Date = new Date();
}
```

- [ ] **Step 3: Write the mappers**

```typescript
// src/wallet/infrastructure/persistence/mappers/wallet.mapper.ts
import { Money } from "../../../../shared-kernel/money";
import { Wallet } from "../../../domain/wallet";
import { WalletLedgerEntry } from "../../../domain/wallet-ledger-entry";
import { WalletEntity } from "../entities/wallet.entity";
import { WalletLedgerEntryEntity } from "../entities/wallet-ledger-entry.entity";

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
      direction: entity.direction,
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
```

- [ ] **Step 4: Write the repository port and implementation**

```typescript
// src/wallet/application/ports/wallet.repository.ts
import { Money } from "../../../shared-kernel/money";
import { Wallet } from "../../domain/wallet";
import { WalletLedgerEntry } from "../../domain/wallet-ledger-entry";

export interface WalletRepository {
  findById(id: string): Promise<Wallet | null>;
  /** Must be called inside an active transaction — issues SELECT ... FOR UPDATE. */
  findByIdForUpdate(id: string): Promise<Wallet | null>;
  findByPlayerAndCurrency(playerId: string, currency: string): Promise<Wallet | null>;
  save(wallet: Wallet): Promise<void>;
  appendLedgerEntry(entry: WalletLedgerEntry): Promise<void>;
  listLedgerEntries(walletId: string, cursor: { after?: string; limit: number }): Promise<WalletLedgerEntry[]>;
  /** Recomputes the balance directly in SQL from every ledger row — the reconciliation source of truth. */
  sumLedgerEntries(walletId: string, currency: string): Promise<{ balance: Money; count: number }>;
}

export const WALLET_REPOSITORY = Symbol("WALLET_REPOSITORY");
```

```typescript
// src/wallet/infrastructure/persistence/repositories/wallet.repository.ts
import { Injectable } from "@nestjs/common";
import { EntityManager, LockMode } from "@mikro-orm/postgresql";
import { Money } from "../../../../shared-kernel/money";
import { Wallet } from "../../../domain/wallet";
import { WalletLedgerEntry } from "../../../domain/wallet-ledger-entry";
import { WalletRepository } from "../../../application/ports/wallet.repository";
import { WalletEntity } from "../entities/wallet.entity";
import { WalletLedgerEntryEntity } from "../entities/wallet-ledger-entry.entity";
import { WalletMapper, WalletLedgerEntryMapper } from "../mappers/wallet.mapper";

@Injectable()
export class MikroOrmWalletRepository implements WalletRepository {
  constructor(private readonly em: EntityManager) {}

  async findById(id: string): Promise<Wallet | null> {
    const entity = await this.em.findOne(WalletEntity, { id });
    return entity ? WalletMapper.toDomain(entity) : null;
  }

  async findByIdForUpdate(id: string): Promise<Wallet | null> {
    const entity = await this.em.findOne(
      WalletEntity,
      { id },
      { lockMode: LockMode.PESSIMISTIC_WRITE },
    );
    return entity ? WalletMapper.toDomain(entity) : null;
  }

  async findByPlayerAndCurrency(playerId: string, currency: string): Promise<Wallet | null> {
    const entity = await this.em.findOne(WalletEntity, { playerId, currency });
    return entity ? WalletMapper.toDomain(entity) : null;
  }

  async save(wallet: Wallet): Promise<void> {
    const existing = await this.em.findOne(WalletEntity, { id: wallet.id });
    const entity = WalletMapper.toEntity(wallet, existing ?? undefined);
    this.em.persist(entity);
  }

  async appendLedgerEntry(entry: WalletLedgerEntry): Promise<void> {
    this.em.persist(WalletLedgerEntryMapper.toEntity(entry));
  }

  async listLedgerEntries(
    walletId: string,
    cursor: { after?: string; limit: number },
  ): Promise<WalletLedgerEntry[]> {
    const entities = await this.em.find(
      WalletLedgerEntryEntity,
      cursor.after ? { walletId, id: { $gt: cursor.after } } : { walletId },
      { orderBy: { id: "asc" }, limit: cursor.limit },
    );
    return entities.map(WalletLedgerEntryMapper.toDomain);
  }

  async sumLedgerEntries(walletId: string, currency: string): Promise<{ balance: Money; count: number }> {
    const rows = (await this.em.getConnection().execute(
      `select
         coalesce(sum(case when direction = 'CREDIT' then amount else -amount end), 0) as balance,
         count(*) as count
       from wallet_ledger_entries
       where wallet_id = $1`,
      [walletId],
    )) as Array<{ balance: string; count: string }>;
    const row = rows[0];
    return {
      balance: Money.rehydrate({ amount: row.balance, currency }),
      count: parseInt(row.count, 10),
    };
  }
}
```

- [ ] **Step 5: Write the failing integration test**

```typescript
// test/wallet-repository-lock.spec.ts
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { Wallet } from "../src/wallet/domain/wallet";
import { Money } from "../src/shared-kernel/money";
import { MikroOrmWalletRepository } from "../src/wallet/infrastructure/persistence/repositories/wallet.repository";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env";

describe("MikroOrmWalletRepository — FOR UPDATE lock", () => {
  let db: TestDatabase;

  beforeAll(async () => {
    db = await startTestDatabase();
  }, 60_000);

  afterAll(async () => {
    await stopTestDatabase(db);
  });

  it("serializes two concurrent findByIdForUpdate calls on the same row", async () => {
    const setupEm = db.orm.em.fork();
    const repo = new MikroOrmWalletRepository(setupEm);
    const { wallet } = Wallet.open({
      id: "11111111-1111-1111-1111-111111111111",
      playerId: "player-1",
      currency: "BRL",
      initialBalance: Money.from({ amount: "100.00", currency: "BRL" }),
      openingTransactionId: "22222222-2222-2222-2222-222222222222",
    });
    // wager_transactions has a FK from wallet_ledger_entries — insert a placeholder row
    // for the opening transaction directly so the FK is satisfiable in this narrow test.
    await setupEm.getConnection().execute(
      `insert into wager_transactions
        (id, wallet_id, external_transaction_id, provider_id, idempotency_key, payload_hash, kind, status)
       values ($1, $2, 'opening', 'internal', $3, 'n/a', 'OPENING', 'PROCESSED')`,
      ["22222222-2222-2222-2222-222222222222", wallet.id, `opening-${wallet.id}`],
    );
    await repo.save(wallet);
    await setupEm.flush();

    const events: string[] = [];

    async function holdLockFor(ms: number): Promise<void> {
      const em = db.orm.em.fork();
      await em.transactional(async (tx) => {
        const txRepo = new MikroOrmWalletRepository(tx);
        events.push("A:acquire");
        await txRepo.findByIdForUpdate(wallet.id);
        await new Promise((resolve) => setTimeout(resolve, ms));
        events.push("A:release");
      });
    }

    async function acquireAfterWait(): Promise<void> {
      // give holdLockFor a head start so it acquires first
      await new Promise((resolve) => setTimeout(resolve, 20));
      const em = db.orm.em.fork();
      await em.transactional(async (tx) => {
        const txRepo = new MikroOrmWalletRepository(tx);
        events.push("B:waiting");
        await txRepo.findByIdForUpdate(wallet.id);
        events.push("B:acquired");
      });
    }

    await Promise.all([holdLockFor(150), acquireAfterWait()]);

    expect(events.indexOf("B:acquired")).toBeGreaterThan(events.indexOf("A:release"));
  }, 20_000);
});
```

- [ ] **Step 6: Run the test to verify it fails first (before entities/repo existed it would fail to import; now it should pass since Steps 2-4 already wrote the implementation)**

Run: `bun test test/wallet-repository-lock.spec.ts`
Expected: PASS. (This task writes implementation and test together because the
test's only purpose is proving the lock behavior — there is no meaningful
intermediate "red" state beyond a missing-module error.)

- [ ] **Step 7: Commit**

```bash
git add test/support/testcontainers-env.ts src/wallet/infrastructure/persistence/ src/wallet/application/ports/wallet.repository.ts test/wallet-repository-lock.spec.ts
git commit -m "feat: add Wallet persistence layer with FOR UPDATE locking, prove serialization"
```

---

## Task 7: WagerTransaction + IdempotencyKey persistence

**Files:**
- Create: `src/wagering/infrastructure/persistence/entities/wager-transaction.entity.ts`
- Create: `src/wagering/infrastructure/persistence/entities/idempotency-key.entity.ts`
- Create: `src/wagering/infrastructure/persistence/mappers/wager-transaction.mapper.ts`
- Create: `src/wagering/application/ports/wager-transaction.repository.ts`
- Create: `src/wagering/application/ports/idempotency-key.repository.ts`
- Create: `src/wagering/infrastructure/persistence/repositories/wager-transaction.repository.ts`
- Create: `src/wagering/infrastructure/persistence/repositories/idempotency-key.repository.ts`
- Test: `test/wager-transaction-repository.spec.ts`

**Interfaces:**
- Consumes: `WagerTransaction` (Task 3), `Money` (Task 1), schema from Task 5,
  `startTestDatabase`/`stopTestDatabase` (Task 6).
- Produces:
  - `interface WagerTransactionRepository` with `findById`, `findByProviderAndExternalId`,
    `findByIdempotencyKey`, `save`, `findPendingReferencesFor`.
  - `class MikroOrmWagerTransactionRepository implements WagerTransactionRepository`.
  - `interface IdempotencyKeyRepository` with `findByKey(key)`,
    `save(record: { key: string; payloadHash: string; response: unknown })`.
  - `class MikroOrmIdempotencyKeyRepository implements IdempotencyKeyRepository`.

- [ ] **Step 1: Write the entities**

```typescript
// src/wagering/infrastructure/persistence/entities/wager-transaction.entity.ts
import { Entity, PrimaryKey, Property } from "@mikro-orm/postgresql";

@Entity({ tableName: "wager_transactions" })
export class WagerTransactionEntity {
  @PrimaryKey({ type: "uuid" })
  id!: string;

  @Property()
  walletId!: string;

  @Property()
  externalTransactionId!: string;

  @Property()
  providerId!: string;

  @Property()
  idempotencyKey!: string;

  @Property()
  payloadHash!: string;

  @Property()
  kind!: string;

  @Property({ type: "decimal", precision: 19, scale: 4 })
  amount!: string;

  @Property({ length: 3 })
  currency!: string;

  @Property({ nullable: true, type: "text" })
  referenceExternalTransactionId!: string | null;

  @Property()
  status!: string;

  @Property({ nullable: true, type: "text" })
  failureCode!: string | null;

  @Property({ type: "decimal", precision: 19, scale: 4, nullable: true })
  resultBalanceAmount!: string | null;

  @Property({ length: 3, nullable: true })
  resultBalanceCurrency!: string | null;

  @Property({ onCreate: () => new Date() })
  createdAt: Date = new Date();

  @Property({ onCreate: () => new Date(), onUpdate: () => new Date() })
  updatedAt: Date = new Date();
}
```

```typescript
// src/wagering/infrastructure/persistence/entities/idempotency-key.entity.ts
import { Entity, PrimaryKey, Property } from "@mikro-orm/postgresql";

@Entity({ tableName: "idempotency_keys" })
export class IdempotencyKeyEntity {
  @PrimaryKey({ type: "text" })
  key!: string;

  @Property()
  payloadHash!: string;

  @Property({ type: "json" })
  response!: unknown;

  @Property({ onCreate: () => new Date() })
  createdAt: Date = new Date();
}
```

- [ ] **Step 2: Write the mapper**

```typescript
// src/wagering/infrastructure/persistence/mappers/wager-transaction.mapper.ts
import { Money } from "../../../../shared-kernel/money";
import { WagerTransaction, WagerKind, WagerStatus } from "../../../domain/wager-transaction";
import { FailureCode } from "../../../../shared-kernel/failure-code";
import { WagerTransactionEntity } from "../entities/wager-transaction.entity";

export class WagerTransactionMapper {
  static toDomain(entity: WagerTransactionEntity): WagerTransaction {
    return WagerTransaction.rehydrate({
      id: entity.id,
      walletId: entity.walletId,
      externalTransactionId: entity.externalTransactionId,
      providerId: entity.providerId,
      idempotencyKey: entity.idempotencyKey,
      payloadHash: entity.payloadHash,
      kind: entity.kind as WagerKind,
      amount: Money.rehydrate({ amount: entity.amount, currency: entity.currency }),
      referenceExternalTransactionId: entity.referenceExternalTransactionId,
      status: entity.status as WagerStatus,
      failureCode: entity.failureCode as FailureCode | null,
      resultBalance:
        entity.resultBalanceAmount && entity.resultBalanceCurrency
          ? Money.rehydrate({ amount: entity.resultBalanceAmount, currency: entity.resultBalanceCurrency })
          : null,
      createdAt: entity.createdAt,
      updatedAt: entity.updatedAt,
    });
  }

  static toEntity(tx: WagerTransaction, existing?: WagerTransactionEntity): WagerTransactionEntity {
    const entity = existing ?? new WagerTransactionEntity();
    entity.id = tx.id;
    entity.walletId = tx.walletId;
    entity.externalTransactionId = tx.externalTransactionId;
    entity.providerId = tx.providerId;
    entity.idempotencyKey = tx.idempotencyKey;
    entity.payloadHash = tx.payloadHash;
    entity.kind = tx.kind;
    entity.amount = tx.amount.toString();
    entity.currency = tx.amount.currency;
    entity.referenceExternalTransactionId = tx.referenceExternalTransactionId;
    entity.status = tx.status;
    entity.failureCode = tx.failureCode;
    entity.resultBalanceAmount = tx.resultBalance ? tx.resultBalance.toString() : null;
    entity.resultBalanceCurrency = tx.resultBalance ? tx.resultBalance.currency : null;
    return entity;
  }
}
```

- [ ] **Step 3: Write the repository ports**

```typescript
// src/wagering/application/ports/wager-transaction.repository.ts
import { WagerTransaction } from "../../domain/wager-transaction";

export interface WagerTransactionRepository {
  findById(id: string): Promise<WagerTransaction | null>;
  findByProviderAndExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | null>;
  findByIdempotencyKey(idempotencyKey: string): Promise<WagerTransaction | null>;
  save(tx: WagerTransaction): Promise<void>;
  /** PENDING_REFERENCE rows whose reference points at (providerId, externalTransactionId). */
  findPendingReferencesFor(providerId: string, externalTransactionId: string): Promise<WagerTransaction[]>;
}

export const WAGER_TRANSACTION_REPOSITORY = Symbol("WAGER_TRANSACTION_REPOSITORY");
```

```typescript
// src/wagering/application/ports/idempotency-key.repository.ts
export interface IdempotencyKeyRecord {
  key: string;
  payloadHash: string;
  response: unknown;
}

export interface IdempotencyKeyRepository {
  findByKey(key: string): Promise<IdempotencyKeyRecord | null>;
  save(record: IdempotencyKeyRecord): Promise<void>;
}

export const IDEMPOTENCY_KEY_REPOSITORY = Symbol("IDEMPOTENCY_KEY_REPOSITORY");
```

- [ ] **Step 4: Write the repository implementations**

```typescript
// src/wagering/infrastructure/persistence/repositories/wager-transaction.repository.ts
import { Injectable } from "@nestjs/common";
import { EntityManager } from "@mikro-orm/postgresql";
import { WagerTransaction } from "../../../domain/wager-transaction";
import { WagerTransactionRepository } from "../../../application/ports/wager-transaction.repository";
import { WagerTransactionEntity } from "../entities/wager-transaction.entity";
import { WagerTransactionMapper } from "../mappers/wager-transaction.mapper";

@Injectable()
export class MikroOrmWagerTransactionRepository implements WagerTransactionRepository {
  constructor(private readonly em: EntityManager) {}

  async findById(id: string): Promise<WagerTransaction | null> {
    const entity = await this.em.findOne(WagerTransactionEntity, { id });
    return entity ? WagerTransactionMapper.toDomain(entity) : null;
  }

  async findByProviderAndExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | null> {
    const entity = await this.em.findOne(WagerTransactionEntity, { providerId, externalTransactionId });
    return entity ? WagerTransactionMapper.toDomain(entity) : null;
  }

  async findByIdempotencyKey(idempotencyKey: string): Promise<WagerTransaction | null> {
    const entity = await this.em.findOne(WagerTransactionEntity, { idempotencyKey });
    return entity ? WagerTransactionMapper.toDomain(entity) : null;
  }

  async save(tx: WagerTransaction): Promise<void> {
    const existing = await this.em.findOne(WagerTransactionEntity, { id: tx.id });
    const entity = WagerTransactionMapper.toEntity(tx, existing ?? undefined);
    this.em.persist(entity);
  }

  async findPendingReferencesFor(providerId: string, externalTransactionId: string): Promise<WagerTransaction[]> {
    const entities = await this.em.find(WagerTransactionEntity, {
      providerId,
      referenceExternalTransactionId: externalTransactionId,
      status: "PENDING_REFERENCE",
    });
    return entities.map(WagerTransactionMapper.toDomain);
  }
}
```

```typescript
// src/wagering/infrastructure/persistence/repositories/idempotency-key.repository.ts
import { Injectable } from "@nestjs/common";
import { EntityManager } from "@mikro-orm/postgresql";
import {
  IdempotencyKeyRecord,
  IdempotencyKeyRepository,
} from "../../../application/ports/idempotency-key.repository";
import { IdempotencyKeyEntity } from "../entities/idempotency-key.entity";

@Injectable()
export class MikroOrmIdempotencyKeyRepository implements IdempotencyKeyRepository {
  constructor(private readonly em: EntityManager) {}

  async findByKey(key: string): Promise<IdempotencyKeyRecord | null> {
    const entity = await this.em.findOne(IdempotencyKeyEntity, { key });
    return entity ? { key: entity.key, payloadHash: entity.payloadHash, response: entity.response } : null;
  }

  async save(record: IdempotencyKeyRecord): Promise<void> {
    const entity = new IdempotencyKeyEntity();
    entity.key = record.key;
    entity.payloadHash = record.payloadHash;
    entity.response = record.response;
    this.em.persist(entity);
  }
}
```

- [ ] **Step 5: Write the integration test proving DB-level dedup**

```typescript
// test/wager-transaction-repository.spec.ts
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { WagerTransaction } from "../src/wagering/domain/wager-transaction";
import { Money } from "../src/shared-kernel/money";
import { MikroOrmWagerTransactionRepository } from "../src/wagering/infrastructure/persistence/repositories/wager-transaction.repository";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env";

describe("MikroOrmWagerTransactionRepository — DB-level uniqueness", () => {
  let db: TestDatabase;

  beforeAll(async () => {
    db = await startTestDatabase();
    await db.orm.em.getConnection().execute(
      `insert into wallets (id, player_id, currency) values ($1, 'player-x', 'BRL')`,
      ["33333333-3333-3333-3333-333333333333"],
    );
  }, 60_000);

  afterAll(async () => {
    await stopTestDatabase(db);
  });

  it("rejects a second insert with the same (providerId, externalTransactionId)", async () => {
    const em = db.orm.em.fork();
    const repo = new MikroOrmWagerTransactionRepository(em);

    const first = WagerTransaction.create({
      id: "44444444-4444-4444-4444-444444444444",
      walletId: "33333333-3333-3333-3333-333333333333",
      externalTransactionId: "bet-1",
      providerId: "provider-a",
      idempotencyKey: "idem-1",
      payloadHash: "hash-1",
      kind: "BET",
      amount: Money.from({ amount: "25.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });
    await repo.save(first);
    await em.flush();

    const duplicate = WagerTransaction.create({
      id: "55555555-5555-5555-5555-555555555555",
      walletId: "33333333-3333-3333-3333-333333333333",
      externalTransactionId: "bet-1", // same provider + external id
      providerId: "provider-a",
      idempotencyKey: "idem-2",
      payloadHash: "hash-2",
      kind: "BET",
      amount: Money.from({ amount: "25.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });
    await repo.save(duplicate);
    await expect(em.flush()).rejects.toThrow();
  });
});
```

- [ ] **Step 6: Run the test**

Run: `bun test test/wager-transaction-repository.spec.ts`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/wagering/infrastructure/persistence/ src/wagering/application/ports/ test/wager-transaction-repository.spec.ts
git commit -m "feat: add WagerTransaction and IdempotencyKey persistence"
```

---

## Task 8: Inbox + Outbox persistence (with SKIP LOCKED claim)

**Files:**
- Create: `src/messaging/infrastructure/persistence/entities/inbox-message.entity.ts`
- Create: `src/messaging/infrastructure/persistence/entities/outbox-message.entity.ts`
- Create: `src/messaging/application/ports/inbox.repository.ts`
- Create: `src/messaging/application/ports/outbox.repository.ts`
- Create: `src/messaging/infrastructure/persistence/repositories/inbox.repository.ts`
- Create: `src/messaging/infrastructure/persistence/repositories/outbox.repository.ts`
- Test: `test/outbox-skip-locked.spec.ts`

**Interfaces:**
- Consumes: schema from Task 5, `startTestDatabase`/`stopTestDatabase` (Task 6).
- Produces:
  - `interface InboxRepository` with `exists(consumerName, messageId)`,
    `markProcessed(consumerName, messageId)`.
  - `class MikroOrmInboxRepository implements InboxRepository`.
  - `type OutboxMessageRecord = { id: string; aggregateId: string; eventType: string; payload: unknown; occurredAt: Date; attempts: number }`
  - `interface OutboxRepository` with `append(message)`,
    `processDueBatch(limit: number, publish: (message: OutboxMessageRecord) => Promise<boolean>): Promise<number>`.
  - `class MikroOrmOutboxRepository implements OutboxRepository`.

- [ ] **Step 1: Write the entities**

```typescript
// src/messaging/infrastructure/persistence/entities/inbox-message.entity.ts
import { Entity, PrimaryKey, Property } from "@mikro-orm/postgresql";

@Entity({ tableName: "inbox_messages" })
export class InboxMessageEntity {
  @PrimaryKey({ type: "text" })
  consumerName!: string;

  @PrimaryKey({ type: "text" })
  messageId!: string;

  @Property({ onCreate: () => new Date() })
  processedAt: Date = new Date();
}
```

```typescript
// src/messaging/infrastructure/persistence/entities/outbox-message.entity.ts
import { Entity, PrimaryKey, Property } from "@mikro-orm/postgresql";

@Entity({ tableName: "outbox_messages" })
export class OutboxMessageEntity {
  @PrimaryKey({ type: "uuid" })
  id!: string;

  @Property()
  aggregateId!: string;

  @Property()
  eventType!: string;

  @Property({ type: "json" })
  payload!: unknown;

  @Property()
  occurredAt!: Date;

  @Property()
  attempts: number = 0;

  @Property()
  nextAttemptAt: Date = new Date();

  @Property({ nullable: true, type: "date" })
  publishedAt!: Date | null;
}
```

- [ ] **Step 2: Write the repository ports**

```typescript
// src/messaging/application/ports/inbox.repository.ts
export interface InboxRepository {
  /** Must be called inside the same transaction as the business write it guards. */
  exists(consumerName: string, messageId: string): Promise<boolean>;
  markProcessed(consumerName: string, messageId: string): Promise<void>;
}

export const INBOX_REPOSITORY = Symbol("INBOX_REPOSITORY");
```

```typescript
// src/messaging/application/ports/outbox.repository.ts
export interface OutboxMessageRecord {
  id: string;
  aggregateId: string;
  eventType: string;
  payload: unknown;
  occurredAt: Date;
  attempts: number;
}

export interface OutboxRepository {
  /** Must be called inside the same transaction as the business write it records. */
  append(message: { id: string; aggregateId: string; eventType: string; payload: unknown; occurredAt: Date }): Promise<void>;
  /**
   * Runs one poll cycle in its own transaction: SELECT ... FOR UPDATE SKIP LOCKED
   * claims up to `limit` due rows, `publish` is invoked for each while the lock is
   * held, and the row is marked published or rescheduled before the transaction
   * commits. Safe to call concurrently from multiple publisher instances — no two
   * instances ever claim the same row. Returns how many rows were processed.
   */
  processDueBatch(limit: number, publish: (message: OutboxMessageRecord) => Promise<boolean>): Promise<number>;
}

export const OUTBOX_REPOSITORY = Symbol("OUTBOX_REPOSITORY");
```

- [ ] **Step 3: Write the repository implementations**

```typescript
// src/messaging/infrastructure/persistence/repositories/inbox.repository.ts
import { Injectable } from "@nestjs/common";
import { EntityManager } from "@mikro-orm/postgresql";
import { InboxRepository } from "../../../application/ports/inbox.repository";
import { InboxMessageEntity } from "../entities/inbox-message.entity";

@Injectable()
export class MikroOrmInboxRepository implements InboxRepository {
  constructor(private readonly em: EntityManager) {}

  async exists(consumerName: string, messageId: string): Promise<boolean> {
    const entity = await this.em.findOne(InboxMessageEntity, { consumerName, messageId });
    return entity !== null;
  }

  async markProcessed(consumerName: string, messageId: string): Promise<void> {
    const entity = new InboxMessageEntity();
    entity.consumerName = consumerName;
    entity.messageId = messageId;
    this.em.persist(entity);
  }
}
```

```typescript
// src/messaging/infrastructure/persistence/repositories/outbox.repository.ts
import { Injectable } from "@nestjs/common";
import { EntityManager, MikroORM } from "@mikro-orm/postgresql";
import { OutboxMessageRecord, OutboxRepository } from "../../../application/ports/outbox.repository";
import { OutboxMessageEntity } from "../entities/outbox-message.entity";

@Injectable()
export class MikroOrmOutboxRepository implements OutboxRepository {
  constructor(
    private readonly em: EntityManager,
    private readonly orm: MikroORM,
  ) {}

  async append(message: {
    id: string;
    aggregateId: string;
    eventType: string;
    payload: unknown;
    occurredAt: Date;
  }): Promise<void> {
    const entity = new OutboxMessageEntity();
    entity.id = message.id;
    entity.aggregateId = message.aggregateId;
    entity.eventType = message.eventType;
    entity.payload = message.payload;
    entity.occurredAt = message.occurredAt;
    this.em.persist(entity);
  }

  async processDueBatch(
    limit: number,
    publish: (message: OutboxMessageRecord) => Promise<boolean>,
  ): Promise<number> {
    const fork = this.orm.em.fork();
    let processed = 0;
    await fork.transactional(async (tx) => {
      const rows = await tx.getConnection().execute(
        `select id, aggregate_id, event_type, payload, occurred_at, attempts
         from outbox_messages
         where published_at is null and next_attempt_at <= now()
         order by occurred_at
         limit $1
         for update skip locked`,
        [limit],
      );
      for (const row of rows as Array<Record<string, unknown>>) {
        const record: OutboxMessageRecord = {
          id: row.id as string,
          aggregateId: row.aggregate_id as string,
          eventType: row.event_type as string,
          payload: row.payload,
          occurredAt: row.occurred_at as Date,
          attempts: row.attempts as number,
        };
        const ok = await publish(record);
        if (ok) {
          await tx.getConnection().execute(
            `update outbox_messages set published_at = now() where id = $1`,
            [record.id],
          );
        } else {
          await tx.getConnection().execute(
            `update outbox_messages set attempts = attempts + 1, next_attempt_at = now() + interval '5 seconds' where id = $1`,
            [record.id],
          );
        }
        processed += 1;
      }
    });
    return processed;
  }
}
```

- [ ] **Step 4: Write the failing test**

```typescript
// test/outbox-skip-locked.spec.ts
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { MikroOrmOutboxRepository } from "../src/messaging/infrastructure/persistence/repositories/outbox.repository";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env";

describe("MikroOrmOutboxRepository — SKIP LOCKED", () => {
  let db: TestDatabase;

  beforeAll(async () => {
    db = await startTestDatabase();
  }, 60_000);

  afterAll(async () => {
    await stopTestDatabase(db);
  });

  it("two concurrent publishers never claim the same row", async () => {
    const setupEm = db.orm.em.fork();
    const setupRepo = new MikroOrmOutboxRepository(setupEm, db.orm);
    await setupRepo.append({
      id: "66666666-6666-6666-6666-666666666666",
      aggregateId: "wallet-1",
      eventType: "WagerProcessed",
      payload: { hello: "1" },
      occurredAt: new Date(),
    });
    await setupRepo.append({
      id: "77777777-7777-7777-7777-777777777777",
      aggregateId: "wallet-2",
      eventType: "WagerProcessed",
      payload: { hello: "2" },
      occurredAt: new Date(),
    });
    await setupEm.flush();

    const publishedByInstance1: string[] = [];
    const publishedByInstance2: string[] = [];

    const instance1 = new MikroOrmOutboxRepository(db.orm.em.fork(), db.orm);
    const instance2 = new MikroOrmOutboxRepository(db.orm.em.fork(), db.orm);

    await Promise.all([
      instance1.processDueBatch(1, async (message) => {
        publishedByInstance1.push(message.id);
        return true;
      }),
      instance2.processDueBatch(1, async (message) => {
        publishedByInstance2.push(message.id);
        return true;
      }),
    ]);

    const allPublished = [...publishedByInstance1, ...publishedByInstance2];
    expect(allPublished).toHaveLength(2);
    expect(new Set(allPublished).size).toBe(2); // no row claimed twice
  }, 20_000);
});
```

- [ ] **Step 5: Run the test**

Run: `bun test test/outbox-skip-locked.spec.ts`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/messaging/infrastructure/persistence/ src/messaging/application/ports/ test/outbox-skip-locked.spec.ts
git commit -m "feat: add Inbox and Outbox persistence with SKIP LOCKED batch claiming"
```

---

## Task 9: CreateWalletUseCase + WalletController (POST /wallets) + OPENING test

**Files:**
- Create: `src/wallet/application/use-cases/create-wallet.use-case.ts`
- Create: `src/wallet/infrastructure/http/dto/create-wallet.dto.ts`
- Create: `src/wallet/infrastructure/http/dto/wallet-response.dto.ts`
- Create: `src/wallet/infrastructure/http/wallet.controller.ts`
- Create: `src/wallet/wallet.module.ts`
- Test: `test/wallet-opening.spec.ts`

**Interfaces:**
- Consumes: `Wallet`, `WalletRepository` (Task 6), `WagerTransaction`,
  `WagerTransactionRepository` (Task 7), `computePayloadHash` (Task 3),
  `Money` (Task 1).
- Produces:
  - `class CreateWalletUseCase` with `execute(input: { playerId: string; currency: string; initialBalance: Money }): Promise<Wallet>`.
  - `class WalletController` — `POST /wallets`.
  - `class WalletModule` (NestJS module wiring the repository tokens to their MikroORM implementations and the use case).

- [ ] **Step 1: Write the use case**

```typescript
// src/wallet/application/use-cases/create-wallet.use-case.ts
import { ConflictException, Inject, Injectable } from "@nestjs/common";
import { EntityManager } from "@mikro-orm/postgresql";
import { Money } from "../../../shared-kernel/money";
import { computePayloadHash } from "../../../wagering/domain/payload-hash";
import { WagerTransaction } from "../../../wagering/domain/wager-transaction";
import {
  WAGER_TRANSACTION_REPOSITORY,
  WagerTransactionRepository,
} from "../../../wagering/application/ports/wager-transaction.repository";
import { Wallet } from "../../domain/wallet";
import { WALLET_REPOSITORY, WalletRepository } from "../ports/wallet.repository";

@Injectable()
export class CreateWalletUseCase {
  constructor(
    private readonly em: EntityManager,
    @Inject(WALLET_REPOSITORY) private readonly walletRepository: WalletRepository,
    @Inject(WAGER_TRANSACTION_REPOSITORY)
    private readonly wagerTransactionRepository: WagerTransactionRepository,
  ) {}

  async execute(input: { playerId: string; currency: string; initialBalance: Money }): Promise<Wallet> {
    return this.em.transactional(async () => {
      const existing = await this.walletRepository.findByPlayerAndCurrency(input.playerId, input.currency);
      if (existing) {
        throw new ConflictException(
          `Wallet already exists for player ${input.playerId} in ${input.currency}`,
        );
      }

      const walletId = crypto.randomUUID();
      const openingTransactionId = crypto.randomUUID();
      const { wallet, openingEntry } = Wallet.open({
        id: walletId,
        playerId: input.playerId,
        currency: input.currency,
        initialBalance: input.initialBalance,
        openingTransactionId,
      });

      if (openingEntry) {
        const openingTx = WagerTransaction.create({
          id: openingTransactionId,
          walletId,
          externalTransactionId: `opening-${walletId}`,
          providerId: "internal",
          idempotencyKey: `opening-${walletId}`,
          payloadHash: computePayloadHash({ walletId, initialBalance: input.initialBalance.toJSON() }),
          kind: "OPENING",
          amount: input.initialBalance,
          referenceExternalTransactionId: null,
        }).markProcessed(wallet.balance);
        await this.wagerTransactionRepository.save(openingTx);
        await this.walletRepository.appendLedgerEntry(openingEntry);
      }

      await this.walletRepository.save(wallet);
      return wallet;
    });
  }
}
```

- [ ] **Step 2: Write the DTOs**

```typescript
// src/wallet/infrastructure/http/dto/create-wallet.dto.ts
import { IsNotEmpty, IsOptional, IsString, Length } from "class-validator";

export class CreateWalletDto {
  @IsString()
  @IsNotEmpty()
  playerId!: string;

  @IsString()
  @Length(3, 3)
  currency!: string;

  @IsOptional()
  @IsString()
  initialBalance?: string;
}
```

```typescript
// src/wallet/infrastructure/http/dto/wallet-response.dto.ts
import { Wallet } from "../../../domain/wallet";

export interface WalletResponseDto {
  id: string;
  playerId: string;
  currency: string;
  balance: { amount: string; currency: string };
  version: number;
}

export function toWalletResponseDto(wallet: Wallet): WalletResponseDto {
  return {
    id: wallet.id,
    playerId: wallet.playerId,
    currency: wallet.currency,
    balance: wallet.balance.toJSON(),
    version: wallet.version,
  };
}
```

- [ ] **Step 3: Write the controller**

```typescript
// src/wallet/infrastructure/http/wallet.controller.ts
import { Body, Controller, Post } from "@nestjs/common";
import { Money } from "../../../shared-kernel/money";
import { CreateWalletUseCase } from "../../application/use-cases/create-wallet.use-case";
import { CreateWalletDto } from "./dto/create-wallet.dto";
import { toWalletResponseDto, WalletResponseDto } from "./dto/wallet-response.dto";

@Controller("wallets")
export class WalletController {
  constructor(private readonly createWalletUseCase: CreateWalletUseCase) {}

  @Post()
  async create(@Body() dto: CreateWalletDto): Promise<WalletResponseDto> {
    const initialBalance = Money.from({ amount: dto.initialBalance ?? "0.00", currency: dto.currency });
    const wallet = await this.createWalletUseCase.execute({
      playerId: dto.playerId,
      currency: dto.currency,
      initialBalance,
    });
    return toWalletResponseDto(wallet);
  }
}
```

- [ ] **Step 4: Write the module**

```typescript
// src/wallet/wallet.module.ts
import { Module } from "@nestjs/common";
import { CreateWalletUseCase } from "./application/use-cases/create-wallet.use-case";
import { WALLET_REPOSITORY } from "./application/ports/wallet.repository";
import { MikroOrmWalletRepository } from "./infrastructure/persistence/repositories/wallet.repository";
import { WalletController } from "./infrastructure/http/wallet.controller";

@Module({
  controllers: [WalletController],
  providers: [
    CreateWalletUseCase,
    { provide: WALLET_REPOSITORY, useClass: MikroOrmWalletRepository },
  ],
  exports: [WALLET_REPOSITORY],
})
export class WalletModule {}
```

- [ ] **Step 5: Write the failing test**

```typescript
// test/wallet-opening.spec.ts
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { EntityManager } from "@mikro-orm/postgresql";
import { Money } from "../src/shared-kernel/money";
import { CreateWalletUseCase } from "../src/wallet/application/use-cases/create-wallet.use-case";
import { MikroOrmWalletRepository } from "../src/wallet/infrastructure/persistence/repositories/wallet.repository";
import { MikroOrmWagerTransactionRepository } from "../src/wagering/infrastructure/persistence/repositories/wager-transaction.repository";
import { WagerTransactionEntity } from "../src/wagering/infrastructure/persistence/entities/wager-transaction.entity";
import { WalletLedgerEntryEntity } from "../src/wallet/infrastructure/persistence/entities/wallet-ledger-entry.entity";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env";

function buildUseCase(em: EntityManager): CreateWalletUseCase {
  return new CreateWalletUseCase(
    em,
    new MikroOrmWalletRepository(em),
    new MikroOrmWagerTransactionRepository(em),
  );
}

describe("CreateWalletUseCase — OPENING transaction", () => {
  let db: TestDatabase;

  beforeAll(async () => {
    db = await startTestDatabase();
  }, 60_000);

  afterAll(async () => {
    await stopTestDatabase(db);
  });

  it("creates an OPENING ledger entry when initial balance is greater than zero", async () => {
    const em = db.orm.em.fork();
    const useCase = buildUseCase(em);
    const wallet = await useCase.execute({
      playerId: "player-opening-1",
      currency: "BRL",
      initialBalance: Money.from({ amount: "100.00", currency: "BRL" }),
    });

    expect(wallet.balance.toString()).toBe("100.00");
    expect(wallet.version).toBe(1);

    const verifyEm = db.orm.em.fork();
    const ledgerRows = await verifyEm.find(WalletLedgerEntryEntity, { walletId: wallet.id });
    expect(ledgerRows).toHaveLength(1);
    expect(ledgerRows[0].direction).toBe("CREDIT");

    const openingTx = await verifyEm.findOne(WagerTransactionEntity, { walletId: wallet.id, kind: "OPENING" });
    expect(openingTx?.status).toBe("PROCESSED");
  });

  it("creates no ledger entry when initial balance is zero", async () => {
    const em = db.orm.em.fork();
    const useCase = buildUseCase(em);
    const wallet = await useCase.execute({
      playerId: "player-opening-2",
      currency: "BRL",
      initialBalance: Money.zero("BRL"),
    });

    const verifyEm = db.orm.em.fork();
    const ledgerRows = await verifyEm.find(WalletLedgerEntryEntity, { walletId: wallet.id });
    expect(ledgerRows).toHaveLength(0);
  });

  it("rejects creating the same (playerId, currency) wallet twice", async () => {
    const em1 = db.orm.em.fork();
    await buildUseCase(em1).execute({
      playerId: "player-opening-3",
      currency: "BRL",
      initialBalance: Money.zero("BRL"),
    });

    const em2 = db.orm.em.fork();
    await expect(
      buildUseCase(em2).execute({
        playerId: "player-opening-3",
        currency: "BRL",
        initialBalance: Money.zero("BRL"),
      }),
    ).rejects.toThrow();
  });
});
```

- [ ] **Step 6: Run the test**

Run: `bun test test/wallet-opening.spec.ts`
Expected: PASS (3 tests).

- [ ] **Step 7: Commit**

```bash
git add src/wallet/application/use-cases/create-wallet.use-case.ts src/wallet/infrastructure/http/ src/wallet/wallet.module.ts test/wallet-opening.spec.ts
git commit -m "feat: add CreateWalletUseCase and POST /wallets endpoint"
```

---

## Task 10: ProcessWagerUseCase — the core orchestration

**Files:**
- Create: `src/wagering/application/use-cases/process-wager.use-case.ts`
- Test: `test/process-wager.spec.ts`

**Interfaces:**
- Consumes: `Wallet`, `WalletRepository` (Tasks 2, 6), `WagerTransaction`,
  `validateReferenceKind`, `WagerTransactionRepository` (Tasks 3, 7),
  `OutboxRepository` (Task 8), `CreateWalletUseCase` (Task 9, test-only, to
  seed wallets).
- Produces:
  - `type ProcessWagerInput = { externalTransactionId: string; providerId: string; idempotencyKey: string; payloadHash: string; kind: WagerKind; walletId: string; amount: Money; referenceExternalTransactionId: string | null }`
  - `type ProcessWagerResult = ({ status: "PROCESSED"; transactionId: string; balance: Money } | { status: "REJECTED"; transactionId: string; failureCode: FailureCode } | { status: "PENDING_REFERENCE"; transactionId: string }) & { idempotentReplay: boolean }`
  - `class ProcessWagerUseCase` with `execute(input: ProcessWagerInput): Promise<ProcessWagerResult>` — **this is the single entry point HTTP and SQS both call** (Task 11 controller, Task 15 consumer). Also handles the race where two concurrent calls for the same `(providerId, externalTransactionId)` both pass the initial dedup check before either commits: the loser's commit fails with a unique-constraint violation, caught and converted into a replay of the winner's result (exercised under real 50-way concurrency in Task 19).

- [ ] **Step 1: Write the use case**

```typescript
// src/wagering/application/use-cases/process-wager.use-case.ts
import { Inject, Injectable, NotFoundException } from "@nestjs/common";
import { EntityManager, UniqueConstraintViolationException } from "@mikro-orm/postgresql";
import { Money } from "../../../shared-kernel/money";
import { FailureCode } from "../../../shared-kernel/failure-code";
import { Wallet, WalletApplyResult } from "../../../wallet/domain/wallet";
import {
  WALLET_REPOSITORY,
  WalletRepository,
} from "../../../wallet/application/ports/wallet.repository";
import { OUTBOX_REPOSITORY, OutboxRepository } from "../../../messaging/application/ports/outbox.repository";
import { WagerKind, WagerTransaction, validateReferenceKind } from "../../domain/wager-transaction";
import {
  WAGER_TRANSACTION_REPOSITORY,
  WagerTransactionRepository,
} from "../ports/wager-transaction.repository";

export interface ProcessWagerInput {
  externalTransactionId: string;
  providerId: string;
  idempotencyKey: string;
  payloadHash: string;
  kind: WagerKind;
  walletId: string;
  amount: Money;
  referenceExternalTransactionId: string | null;
}

export type ProcessWagerResult = (
  | { status: "PROCESSED"; transactionId: string; balance: Money }
  | { status: "REJECTED"; transactionId: string; failureCode: FailureCode }
  | { status: "PENDING_REFERENCE"; transactionId: string }
) & { idempotentReplay: boolean };

function originalDirectionOf(kind: WagerKind): "DEBIT" | "CREDIT" {
  return kind === "BET" || kind === "LOSS" ? "DEBIT" : "CREDIT";
}

@Injectable()
export class ProcessWagerUseCase {
  constructor(
    private readonly em: EntityManager,
    @Inject(WALLET_REPOSITORY) private readonly walletRepository: WalletRepository,
    @Inject(WAGER_TRANSACTION_REPOSITORY)
    private readonly wagerTransactionRepository: WagerTransactionRepository,
    @Inject(OUTBOX_REPOSITORY) private readonly outboxRepository: OutboxRepository,
  ) {}

  async execute(input: ProcessWagerInput): Promise<ProcessWagerResult> {
    try {
      // em.transactional() runs the callback with this same EntityManager switched into an
      // active DB transaction — every repository built from `this.em` (injected via Nest DI,
      // shared across this use case's constructor) automatically participates in it.
      return await this.em.transactional(() => this.executeWithinTransaction(input));
    } catch (err) {
      if (err instanceof UniqueConstraintViolationException) {
        // Another concurrent request for the same (providerId, externalTransactionId) won the
        // race and committed first — this is the expected outcome of "same bet sent N times in
        // parallel": exactly one debit, everyone else replays. Re-read the now-committed row.
        const winner = await this.wagerTransactionRepository.findByProviderAndExternalId(
          input.providerId,
          input.externalTransactionId,
        );
        if (winner) {
          return { ...this.toResult(winner), idempotentReplay: true };
        }
      }
      throw err;
    }
  }

  private async executeWithinTransaction(input: ProcessWagerInput): Promise<ProcessWagerResult> {
    const existing = await this.wagerTransactionRepository.findByProviderAndExternalId(
      input.providerId,
      input.externalTransactionId,
    );
    if (existing) {
      return { ...this.toResult(existing), idempotentReplay: true };
    }

    let referencedTx: WagerTransaction | null = null;

      if (input.kind === "REFUND" || input.kind === "ROLLBACK") {
        if (!input.referenceExternalTransactionId) {
          const tx = this.newTransaction(input).markRejected(FailureCode.REFERENCE_NOT_FOUND);
          await this.wagerTransactionRepository.save(tx);
          return {
            status: "REJECTED",
            transactionId: tx.id,
            failureCode: FailureCode.REFERENCE_NOT_FOUND,
            idempotentReplay: false,
          };
        }

        referencedTx = await this.wagerTransactionRepository.findByProviderAndExternalId(
          input.providerId,
          input.referenceExternalTransactionId,
        );

        if (!referencedTx || referencedTx.status !== "PROCESSED") {
          const tx = this.newTransaction(input).markPendingReference();
          await this.wagerTransactionRepository.save(tx);
          return { status: "PENDING_REFERENCE", transactionId: tx.id, idempotentReplay: false };
        }

        const invalidRefCode = validateReferenceKind(input.kind, referencedTx.kind);
        if (invalidRefCode) {
          const tx = this.newTransaction(input).markRejected(invalidRefCode);
          await this.wagerTransactionRepository.save(tx);
          return { status: "REJECTED", transactionId: tx.id, failureCode: invalidRefCode, idempotentReplay: false };
        }
      }

      let wallet = await this.walletRepository.findByIdForUpdate(input.walletId);
      if (!wallet) {
        throw new NotFoundException(`Wallet ${input.walletId} not found`);
      }

      const tx = this.newTransaction(input);
      const effectiveAmount = referencedTx ? referencedTx.amount : input.amount;
      const applyResult = this.applyToWallet(wallet, input.kind, referencedTx?.kind ?? null, effectiveAmount, tx.id);

      if (applyResult.status === "REJECTED") {
        await this.wagerTransactionRepository.save(tx.markRejected(applyResult.failureCode));
        return {
          status: "REJECTED",
          transactionId: tx.id,
          failureCode: applyResult.failureCode,
          idempotentReplay: false,
        };
      }

      wallet = applyResult.wallet;
      const processedTx = tx.markProcessed(wallet.balance);
      await this.wagerTransactionRepository.save(processedTx);
      await this.walletRepository.appendLedgerEntry(applyResult.entry);
      await this.walletRepository.save(wallet);
      await this.publishWagerProcessed(processedTx, wallet);

      wallet = await this.resolvePendingReferences(wallet, processedTx);

    return { status: "PROCESSED", transactionId: tx.id, balance: wallet.balance, idempotentReplay: false };
  }

  private newTransaction(input: ProcessWagerInput): WagerTransaction {
    return WagerTransaction.create({
      id: crypto.randomUUID(),
      walletId: input.walletId,
      externalTransactionId: input.externalTransactionId,
      providerId: input.providerId,
      idempotencyKey: input.idempotencyKey,
      payloadHash: input.payloadHash,
      kind: input.kind,
      amount: input.amount,
      referenceExternalTransactionId: input.referenceExternalTransactionId,
    });
  }

  private applyToWallet(
    wallet: Wallet,
    kind: WagerKind,
    referencedKind: WagerKind | null,
    amount: Money,
    transactionId: string,
  ): WalletApplyResult {
    const isDebit = referencedKind
      ? originalDirectionOf(referencedKind) === "CREDIT" // a reversal flips the original direction
      : originalDirectionOf(kind) === "DEBIT";
    return isDebit ? wallet.debit({ amount, transactionId }) : wallet.credit({ amount, transactionId });
  }

  private async publishWagerProcessed(tx: WagerTransaction, wallet: Wallet): Promise<void> {
    await this.outboxRepository.append({
      id: crypto.randomUUID(),
      aggregateId: wallet.id,
      eventType: "WagerProcessed",
      payload: {
        transactionId: tx.id,
        walletId: wallet.id,
        kind: tx.kind,
        balance: wallet.balance.toJSON(),
      },
      occurredAt: new Date(),
    });
  }

  /** Resolves REFUND/ROLLBACK rows that were waiting on the transaction just processed. */
  private async resolvePendingReferences(wallet: Wallet, justProcessed: WagerTransaction): Promise<Wallet> {
    const pendingRows = await this.wagerTransactionRepository.findPendingReferencesFor(
      justProcessed.providerId,
      justProcessed.externalTransactionId,
    );
    let currentWallet = wallet;
    for (const pending of pendingRows) {
      const invalidRefCode = validateReferenceKind(pending.kind, justProcessed.kind);
      if (invalidRefCode) {
        await this.wagerTransactionRepository.save(pending.markRejected(invalidRefCode));
        continue;
      }
      const applyResult = this.applyToWallet(
        currentWallet,
        pending.kind,
        justProcessed.kind,
        justProcessed.amount,
        pending.id,
      );
      if (applyResult.status === "REJECTED") {
        await this.wagerTransactionRepository.save(pending.markRejected(applyResult.failureCode));
        continue;
      }
      currentWallet = applyResult.wallet;
      await this.wagerTransactionRepository.save(pending.markProcessed(currentWallet.balance));
      await this.walletRepository.appendLedgerEntry(applyResult.entry);
      await this.walletRepository.save(currentWallet);
      await this.publishWagerProcessed(pending.markProcessed(currentWallet.balance), currentWallet);
    }
    return currentWallet;
  }

  private toResult(tx: WagerTransaction): Omit<ProcessWagerResult, "idempotentReplay"> {
    if (tx.status === "PROCESSED") {
      return { status: "PROCESSED", transactionId: tx.id, balance: tx.resultBalance! };
    }
    if (tx.status === "PENDING_REFERENCE") {
      return { status: "PENDING_REFERENCE", transactionId: tx.id };
    }
    return { status: "REJECTED", transactionId: tx.id, failureCode: tx.failureCode! };
  }
}
```

- [ ] **Step 2: Write the failing integration tests**

```typescript
// test/process-wager.spec.ts
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { EntityManager } from "@mikro-orm/postgresql";
import { Money } from "../src/shared-kernel/money";
import { FailureCode } from "../src/shared-kernel/failure-code";
import { CreateWalletUseCase } from "../src/wallet/application/use-cases/create-wallet.use-case";
import { MikroOrmWalletRepository } from "../src/wallet/infrastructure/persistence/repositories/wallet.repository";
import { MikroOrmWagerTransactionRepository } from "../src/wagering/infrastructure/persistence/repositories/wager-transaction.repository";
import { MikroOrmOutboxRepository } from "../src/messaging/infrastructure/persistence/repositories/outbox.repository";
import { OutboxMessageEntity } from "../src/messaging/infrastructure/persistence/entities/outbox-message.entity";
import { ProcessWagerUseCase } from "../src/wagering/application/use-cases/process-wager.use-case";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env";

function buildProcessWagerUseCase(em: EntityManager): ProcessWagerUseCase {
  return new ProcessWagerUseCase(
    em,
    new MikroOrmWalletRepository(em),
    new MikroOrmWagerTransactionRepository(em),
    new MikroOrmOutboxRepository(em, undefined as never), // orm not needed for append()
  );
}

async function seedWallet(db: TestDatabase, playerId: string, initial: string): Promise<string> {
  const em = db.orm.em.fork();
  const useCase = new CreateWalletUseCase(em, new MikroOrmWalletRepository(em), new MikroOrmWagerTransactionRepository(em));
  const wallet = await useCase.execute({ playerId, currency: "BRL", initialBalance: Money.from({ amount: initial, currency: "BRL" }) });
  return wallet.id;
}

describe("ProcessWagerUseCase", () => {
  let db: TestDatabase;

  beforeAll(async () => {
    db = await startTestDatabase();
  }, 60_000);

  afterAll(async () => {
    await stopTestDatabase(db);
  });

  it("processes a BET, debits the wallet and writes an outbox event", async () => {
    const walletId = await seedWallet(db, "player-bet-1", "100.00");
    const em = db.orm.em.fork();
    const result = await buildProcessWagerUseCase(em).execute({
      externalTransactionId: "bet-1",
      providerId: "provider-a",
      idempotencyKey: "idem-bet-1",
      payloadHash: "hash-bet-1",
      kind: "BET",
      walletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });

    expect(result).toMatchObject({ status: "PROCESSED", idempotentReplay: false });
    if (result.status === "PROCESSED") {
      expect(result.balance.toString()).toBe("70.00");
    }

    const verifyEm = db.orm.em.fork();
    const outboxRows = await verifyEm.find(OutboxMessageEntity, { aggregateId: walletId });
    expect(outboxRows).toHaveLength(1);
    expect(outboxRows[0].publishedAt).toBeNull();
  });

  it("rejects a BET with insufficient funds without touching the balance", async () => {
    const walletId = await seedWallet(db, "player-bet-2", "10.00");
    const em = db.orm.em.fork();
    const result = await buildProcessWagerUseCase(em).execute({
      externalTransactionId: "bet-2",
      providerId: "provider-a",
      idempotencyKey: "idem-bet-2",
      payloadHash: "hash-bet-2",
      kind: "BET",
      walletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });

    expect(result).toEqual({
      status: "REJECTED",
      transactionId: expect.any(String),
      failureCode: FailureCode.INSUFFICIENT_FUNDS,
      idempotentReplay: false,
    });
  });

  it("replays the original result for a duplicate (providerId, externalTransactionId) without a second debit", async () => {
    const walletId = await seedWallet(db, "player-bet-3", "100.00");
    const input = {
      externalTransactionId: "bet-3",
      providerId: "provider-a",
      idempotencyKey: "idem-bet-3",
      payloadHash: "hash-bet-3",
      kind: "BET" as const,
      walletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    };

    const first = await buildProcessWagerUseCase(db.orm.em.fork()).execute(input);
    const second = await buildProcessWagerUseCase(db.orm.em.fork()).execute(input);

    expect(first.idempotentReplay).toBe(false);
    expect(second.idempotentReplay).toBe(true);
    expect(second).toMatchObject({ status: "PROCESSED" });
    if (first.status === "PROCESSED" && second.status === "PROCESSED") {
      expect(second.balance.toString()).toBe(first.balance.toString());
    }

    const verifyEm = db.orm.em.fork();
    const wallet = await new MikroOrmWalletRepository(verifyEm).findById(walletId);
    expect(wallet?.balance.toString()).toBe("70.00"); // debited exactly once
  });

  it("a REFUND arriving before its BET is PENDING_REFERENCE, then resolves when the BET arrives", async () => {
    const walletId = await seedWallet(db, "player-refund-1", "100.00");

    const refundResult = await buildProcessWagerUseCase(db.orm.em.fork()).execute({
      externalTransactionId: "refund-1",
      providerId: "provider-a",
      idempotencyKey: "idem-refund-1",
      payloadHash: "hash-refund-1",
      kind: "REFUND",
      walletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: "bet-not-yet-arrived",
    });
    expect(refundResult).toMatchObject({ status: "PENDING_REFERENCE" });

    const betResult = await buildProcessWagerUseCase(db.orm.em.fork()).execute({
      externalTransactionId: "bet-not-yet-arrived",
      providerId: "provider-a",
      idempotencyKey: "idem-bet-x",
      payloadHash: "hash-bet-x",
      kind: "BET",
      walletId,
      amount: Money.from({ amount: "30.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });
    expect(betResult).toMatchObject({ status: "PROCESSED" });
    if (betResult.status === "PROCESSED") {
      // BET debited 30 (100 -> 70), then the now-resolved REFUND credited 30 back (70 -> 100)
      expect(betResult.balance.toString()).toBe("100.00");
    }

    const verifyEm = db.orm.em.fork();
    const wallet = await new MikroOrmWalletRepository(verifyEm).findById(walletId);
    expect(wallet?.balance.toString()).toBe("100.00");
  });
});
```

- [ ] **Step 3: Run the tests**

Run: `bun test test/process-wager.spec.ts`
Expected: PASS (4 tests).

- [ ] **Step 4: Commit**

```bash
git add src/wagering/application/use-cases/process-wager.use-case.ts test/process-wager.spec.ts
git commit -m "feat: add ProcessWagerUseCase — the single entry point for HTTP and SQS"
```

---

## Task 11: HTTP idempotency service + WageringController (POST /wagering/transactions)

**Files:**
- Create: `src/wagering/infrastructure/http/idempotency.service.ts`
- Create: `src/wagering/infrastructure/http/dto/submit-wager.dto.ts`
- Create: `src/wagering/infrastructure/http/dto/wager-response.dto.ts`
- Create: `src/wagering/infrastructure/http/wagering.controller.ts`
- Test: `test/idempotency-replay.spec.ts`

**Interfaces:**
- Consumes: `IdempotencyKeyRepository` (Task 7), `computePayloadHash` (Task 3),
  `ProcessWagerUseCase` (Task 10), `Money` (Task 1).
- Produces:
  - `class IdempotencyService` with `handle<T>(key: string, businessFields: Record<string, unknown>, execute: () => Promise<T>): Promise<{ response: T; idempotentReplay: boolean }>`.
  - `class SubmitWagerDto`.
  - `toWagerResponseDto(result: ProcessWagerResult, idempotentReplay: boolean): WagerResponseDto`.
  - `class WageringController` — `POST /wagering/transactions`.

- [ ] **Step 1: Write the idempotency service**

```typescript
// src/wagering/infrastructure/http/idempotency.service.ts
import { ConflictException, Inject, Injectable } from "@nestjs/common";
import { computePayloadHash } from "../../domain/payload-hash";
import {
  IDEMPOTENCY_KEY_REPOSITORY,
  IdempotencyKeyRepository,
} from "../../application/ports/idempotency-key.repository";

@Injectable()
export class IdempotencyService {
  constructor(
    @Inject(IDEMPOTENCY_KEY_REPOSITORY) private readonly idempotencyKeyRepository: IdempotencyKeyRepository,
  ) {}

  /**
   * Fast-path HTTP replay cache. Even if this were skipped entirely, replaying
   * the same (providerId, externalTransactionId) still resolves correctly
   * through ProcessWagerUseCase's own domain-level uniqueness check — this
   * service exists to return the *exact original response bytes* without
   * re-running any logic, and to reject a reused key sent with a different
   * payload.
   */
  async handle<T>(
    key: string,
    businessFields: Record<string, unknown>,
    execute: () => Promise<T>,
  ): Promise<{ response: T; idempotentReplay: boolean }> {
    const payloadHash = computePayloadHash(businessFields);
    const existing = await this.idempotencyKeyRepository.findByKey(key);
    if (existing) {
      if (existing.payloadHash !== payloadHash) {
        throw new ConflictException(
          `Idempotency-Key ${key} was already used with a different payload`,
        );
      }
      return { response: existing.response as T, idempotentReplay: true };
    }

    const response = await execute();
    await this.idempotencyKeyRepository.save({ key, payloadHash, response });
    return { response, idempotentReplay: false };
  }
}
```

- [ ] **Step 2: Write the DTOs**

```typescript
// src/wagering/infrastructure/http/dto/submit-wager.dto.ts
import { IsIn, IsNotEmpty, IsOptional, IsString, Length } from "class-validator";

const SUBMITTABLE_KINDS = ["BET", "WIN", "LOSS", "REFUND", "ROLLBACK"] as const;

export class SubmitWagerDto {
  @IsString()
  @IsNotEmpty()
  externalTransactionId!: string;

  @IsString()
  @IsNotEmpty()
  providerId!: string;

  @IsString()
  @IsNotEmpty()
  walletId!: string;

  @IsIn(SUBMITTABLE_KINDS)
  kind!: (typeof SUBMITTABLE_KINDS)[number];

  @IsString()
  @IsNotEmpty()
  amount!: string;

  @IsString()
  @Length(3, 3)
  currency!: string;

  @IsOptional()
  @IsString()
  referenceExternalTransactionId?: string;
}
```

```typescript
// src/wagering/infrastructure/http/dto/wager-response.dto.ts
import { ProcessWagerResult } from "../../application/use-cases/process-wager.use-case";

export interface WagerResponseDto {
  transactionId: string;
  status: "PROCESSED" | "REJECTED" | "PENDING_REFERENCE";
  idempotentReplay: boolean;
  balance?: { amount: string; currency: string };
  failureCode?: string;
}

export function toWagerResponseDto(result: ProcessWagerResult, idempotentReplay: boolean): WagerResponseDto {
  const base = { transactionId: result.transactionId, status: result.status, idempotentReplay };
  if (result.status === "PROCESSED") {
    return { ...base, balance: result.balance.toJSON() };
  }
  if (result.status === "REJECTED") {
    return { ...base, failureCode: result.failureCode };
  }
  return base;
}
```

- [ ] **Step 3: Write the controller**

```typescript
// src/wagering/infrastructure/http/wagering.controller.ts
import { BadRequestException, Body, Controller, Headers, Post } from "@nestjs/common";
import { Money } from "../../../shared-kernel/money";
import { computePayloadHash } from "../../domain/payload-hash";
import { ProcessWagerUseCase } from "../../application/use-cases/process-wager.use-case";
import { IdempotencyService } from "./idempotency.service";
import { SubmitWagerDto } from "./dto/submit-wager.dto";
import { toWagerResponseDto, WagerResponseDto } from "./dto/wager-response.dto";

@Controller("wagering")
export class WageringController {
  constructor(
    private readonly processWagerUseCase: ProcessWagerUseCase,
    private readonly idempotencyService: IdempotencyService,
  ) {}

  @Post("transactions")
  async submit(
    @Headers("idempotency-key") idempotencyKey: string | undefined,
    @Body() dto: SubmitWagerDto,
  ): Promise<WagerResponseDto> {
    if (!idempotencyKey) {
      throw new BadRequestException("Idempotency-Key header is required");
    }

    const businessFields = {
      externalTransactionId: dto.externalTransactionId,
      providerId: dto.providerId,
      walletId: dto.walletId,
      kind: dto.kind,
      amount: dto.amount,
      currency: dto.currency,
      referenceExternalTransactionId: dto.referenceExternalTransactionId ?? null,
    };
    const payloadHash = computePayloadHash(businessFields);

    const { response, idempotentReplay } = await this.idempotencyService.handle(
      idempotencyKey,
      businessFields,
      () =>
        this.processWagerUseCase.execute({
          externalTransactionId: dto.externalTransactionId,
          providerId: dto.providerId,
          idempotencyKey,
          payloadHash,
          kind: dto.kind,
          walletId: dto.walletId,
          amount: Money.from({ amount: dto.amount, currency: dto.currency }),
          referenceExternalTransactionId: dto.referenceExternalTransactionId ?? null,
        }),
    );

    return toWagerResponseDto(response, idempotentReplay || response.idempotentReplay);
  }
}
```

- [ ] **Step 4: Write the failing test**

```typescript
// test/idempotency-replay.spec.ts
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { Money } from "../src/shared-kernel/money";
import { CreateWalletUseCase } from "../src/wallet/application/use-cases/create-wallet.use-case";
import { MikroOrmWalletRepository } from "../src/wallet/infrastructure/persistence/repositories/wallet.repository";
import { MikroOrmWagerTransactionRepository } from "../src/wagering/infrastructure/persistence/repositories/wager-transaction.repository";
import { MikroOrmOutboxRepository } from "../src/messaging/infrastructure/persistence/repositories/outbox.repository";
import { MikroOrmIdempotencyKeyRepository } from "../src/wagering/infrastructure/persistence/repositories/idempotency-key.repository";
import { ProcessWagerUseCase } from "../src/wagering/application/use-cases/process-wager.use-case";
import { IdempotencyService } from "../src/wagering/infrastructure/http/idempotency.service";
import { WageringController } from "../src/wagering/infrastructure/http/wagering.controller";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env";

describe("WageringController — HTTP idempotency", () => {
  let db: TestDatabase;
  let walletId: string;

  beforeAll(async () => {
    db = await startTestDatabase();
    const em = db.orm.em.fork();
    const wallet = await new CreateWalletUseCase(
      em,
      new MikroOrmWalletRepository(em),
      new MikroOrmWagerTransactionRepository(em),
    ).execute({ playerId: "player-http-1", currency: "BRL", initialBalance: Money.from({ amount: "100.00", currency: "BRL" }) });
    walletId = wallet.id;
  }, 60_000);

  afterAll(async () => {
    await stopTestDatabase(db);
  });

  function buildController() {
    const em = db.orm.em.fork();
    const processWagerUseCase = new ProcessWagerUseCase(
      em,
      new MikroOrmWalletRepository(em),
      new MikroOrmWagerTransactionRepository(em),
      new MikroOrmOutboxRepository(em, undefined as never),
    );
    const idempotencyService = new IdempotencyService(new MikroOrmIdempotencyKeyRepository(em));
    return new WageringController(processWagerUseCase, idempotencyService);
  }

  const baseDto = {
    externalTransactionId: "http-bet-1",
    providerId: "provider-http",
    kind: "BET" as const,
    amount: "30.00",
    currency: "BRL",
  };

  it("requires the Idempotency-Key header", async () => {
    await expect(
      buildController().submit(undefined, { ...baseDto, walletId }),
    ).rejects.toThrow("Idempotency-Key header is required");
  });

  it("returns idempotentReplay: false on first submission, true on retry with the same key and payload", async () => {
    const first = await buildController().submit("idem-http-1", { ...baseDto, walletId });
    expect(first.idempotentReplay).toBe(false);
    expect(first.status).toBe("PROCESSED");

    const second = await buildController().submit("idem-http-1", { ...baseDto, walletId });
    expect(second.idempotentReplay).toBe(true);
    expect(second).toEqual(first);
  });

  it("rejects the same Idempotency-Key reused with a different payload", async () => {
    await buildController().submit("idem-http-2", { ...baseDto, walletId, externalTransactionId: "http-bet-2" });
    await expect(
      buildController().submit("idem-http-2", {
        ...baseDto,
        walletId,
        externalTransactionId: "http-bet-2",
        amount: "999.00", // different payload, same key
      }),
    ).rejects.toThrow(/different payload/);
  });
});
```

- [ ] **Step 5: Run the tests**

Run: `bun test test/idempotency-replay.spec.ts`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add src/wagering/infrastructure/http/ test/idempotency-replay.spec.ts
git commit -m "feat: add HTTP idempotency service and POST /wagering/transactions"
```

---

## Task 12: Read endpoints — wallet, ledger, transaction lookup

**Files:**
- Modify: `src/wallet/infrastructure/http/wallet.controller.ts`
- Modify: `src/wallet/wallet.module.ts`
- Create: `src/wallet/infrastructure/http/dto/ledger-response.dto.ts`
- Modify: `src/wagering/infrastructure/http/wagering.controller.ts`
- Modify: `src/wagering/wagering.module.ts` (created in this task, wires `WAGER_TRANSACTION_REPOSITORY`, `IDEMPOTENCY_KEY_REPOSITORY`, `ProcessWagerUseCase`, `IdempotencyService`, `WageringController`)
- Test: `test/wallet-reads.spec.ts`

**Interfaces:**
- Consumes: `WalletRepository` (Task 6), `WagerTransactionRepository.findById` (Task 7, patched above).
- Produces:
  - `GET /wallets/:walletId` → `WalletResponseDto`.
  - `GET /wallets/:walletId/ledger?after=&limit=` → `LedgerResponseDto`.
  - `GET /wagering/transactions/:transactionId` → `WagerResponseDto`.
  - `GET /providers/:providerId/wagering/transactions/:externalTransactionId` → `WagerResponseDto`.
  - `class WageringModule` (NestJS module for the wagering feature).

- [ ] **Step 1: Write the ledger response DTO**

```typescript
// src/wallet/infrastructure/http/dto/ledger-response.dto.ts
import { WalletLedgerEntry } from "../../../domain/wallet-ledger-entry";

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
```

- [ ] **Step 2: Extend WalletController with the two GET endpoints**

```typescript
// src/wallet/infrastructure/http/wallet.controller.ts
import { Body, Controller, Get, Inject, NotFoundException, Param, Post, Query } from "@nestjs/common";
import { Money } from "../../../shared-kernel/money";
import { CreateWalletUseCase } from "../../application/use-cases/create-wallet.use-case";
import { WALLET_REPOSITORY, WalletRepository } from "../../application/ports/wallet.repository";
import { CreateWalletDto } from "./dto/create-wallet.dto";
import { toWalletResponseDto, WalletResponseDto } from "./dto/wallet-response.dto";
import { toLedgerResponseDto, LedgerResponseDto } from "./dto/ledger-response.dto";

@Controller("wallets")
export class WalletController {
  constructor(
    private readonly createWalletUseCase: CreateWalletUseCase,
    @Inject(WALLET_REPOSITORY) private readonly walletRepository: WalletRepository,
  ) {}

  @Post()
  async create(@Body() dto: CreateWalletDto): Promise<WalletResponseDto> {
    const initialBalance = Money.from({ amount: dto.initialBalance ?? "0.00", currency: dto.currency });
    const wallet = await this.createWalletUseCase.execute({
      playerId: dto.playerId,
      currency: dto.currency,
      initialBalance,
    });
    return toWalletResponseDto(wallet);
  }

  @Get(":walletId")
  async getWallet(@Param("walletId") walletId: string): Promise<WalletResponseDto> {
    const wallet = await this.walletRepository.findById(walletId);
    if (!wallet) {
      throw new NotFoundException(`Wallet ${walletId} not found`);
    }
    return toWalletResponseDto(wallet);
  }

  @Get(":walletId/ledger")
  async getLedger(
    @Param("walletId") walletId: string,
    @Query("after") after?: string,
    @Query("limit") limit?: string,
  ): Promise<LedgerResponseDto> {
    const parsedLimit = limit ? Math.min(Math.max(parseInt(limit, 10), 1), 100) : 20;
    const entries = await this.walletRepository.listLedgerEntries(walletId, { after, limit: parsedLimit });
    return toLedgerResponseDto(entries, parsedLimit);
  }
}
```

- [ ] **Step 3: Add GET endpoints to WageringController**

```typescript
// src/wagering/infrastructure/http/wagering.controller.ts
// (add to the existing file from Task 11 — full file shown for clarity)
import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Headers,
  Inject,
  NotFoundException,
  Param,
  Post,
} from "@nestjs/common";
import { Money } from "../../../shared-kernel/money";
import { computePayloadHash } from "../../domain/payload-hash";
import { ProcessWagerUseCase } from "../../application/use-cases/process-wager.use-case";
import {
  WAGER_TRANSACTION_REPOSITORY,
  WagerTransactionRepository,
} from "../../application/ports/wager-transaction.repository";
import { IdempotencyService } from "./idempotency.service";
import { SubmitWagerDto } from "./dto/submit-wager.dto";
import { toWagerResponseDto, WagerResponseDto } from "./dto/wager-response.dto";

@Controller()
export class WageringController {
  constructor(
    private readonly processWagerUseCase: ProcessWagerUseCase,
    private readonly idempotencyService: IdempotencyService,
    @Inject(WAGER_TRANSACTION_REPOSITORY)
    private readonly wagerTransactionRepository: WagerTransactionRepository,
  ) {}

  @Post("wagering/transactions")
  async submit(
    @Headers("idempotency-key") idempotencyKey: string | undefined,
    @Body() dto: SubmitWagerDto,
  ): Promise<WagerResponseDto> {
    if (!idempotencyKey) {
      throw new BadRequestException("Idempotency-Key header is required");
    }
    const businessFields = {
      externalTransactionId: dto.externalTransactionId,
      providerId: dto.providerId,
      walletId: dto.walletId,
      kind: dto.kind,
      amount: dto.amount,
      currency: dto.currency,
      referenceExternalTransactionId: dto.referenceExternalTransactionId ?? null,
    };
    const payloadHash = computePayloadHash(businessFields);

    const { response, idempotentReplay } = await this.idempotencyService.handle(
      idempotencyKey,
      businessFields,
      () =>
        this.processWagerUseCase.execute({
          externalTransactionId: dto.externalTransactionId,
          providerId: dto.providerId,
          idempotencyKey,
          payloadHash,
          kind: dto.kind,
          walletId: dto.walletId,
          amount: Money.from({ amount: dto.amount, currency: dto.currency }),
          referenceExternalTransactionId: dto.referenceExternalTransactionId ?? null,
        }),
    );

    return toWagerResponseDto(response, idempotentReplay || response.idempotentReplay);
  }

  @Get("wagering/transactions/:transactionId")
  async getByTransactionId(@Param("transactionId") transactionId: string): Promise<WagerResponseDto> {
    const tx = await this.wagerTransactionRepository.findById(transactionId);
    if (!tx) {
      throw new NotFoundException(`Transaction ${transactionId} not found`);
    }
    return toWagerResponseDto({ ...this.toProcessResult(tx), idempotentReplay: false }, false);
  }

  @Get("providers/:providerId/wagering/transactions/:externalTransactionId")
  async getByExternalId(
    @Param("providerId") providerId: string,
    @Param("externalTransactionId") externalTransactionId: string,
  ): Promise<WagerResponseDto> {
    const tx = await this.wagerTransactionRepository.findByProviderAndExternalId(providerId, externalTransactionId);
    if (!tx) {
      throw new NotFoundException(`Transaction ${externalTransactionId} for provider ${providerId} not found`);
    }
    return toWagerResponseDto({ ...this.toProcessResult(tx), idempotentReplay: false }, false);
  }

  private toProcessResult(tx: Awaited<ReturnType<WagerTransactionRepository["findById"]>> & {}) {
    if (tx!.status === "PROCESSED") {
      return { status: "PROCESSED" as const, transactionId: tx!.id, balance: tx!.resultBalance! };
    }
    if (tx!.status === "PENDING_REFERENCE") {
      return { status: "PENDING_REFERENCE" as const, transactionId: tx!.id };
    }
    return { status: "REJECTED" as const, transactionId: tx!.id, failureCode: tx!.failureCode! };
  }
}
```

- [ ] **Step 4: Write the WageringModule**

```typescript
// src/wagering/wagering.module.ts
import { Module } from "@nestjs/common";
import { ProcessWagerUseCase } from "./application/use-cases/process-wager.use-case";
import { WAGER_TRANSACTION_REPOSITORY } from "./application/ports/wager-transaction.repository";
import { IDEMPOTENCY_KEY_REPOSITORY } from "./application/ports/idempotency-key.repository";
import { MikroOrmWagerTransactionRepository } from "./infrastructure/persistence/repositories/wager-transaction.repository";
import { MikroOrmIdempotencyKeyRepository } from "./infrastructure/persistence/repositories/idempotency-key.repository";
import { IdempotencyService } from "./infrastructure/http/idempotency.service";
import { WageringController } from "./infrastructure/http/wagering.controller";
import { WalletModule } from "../wallet/wallet.module";
import { MessagingModule } from "../messaging/messaging.module";

@Module({
  imports: [WalletModule, MessagingModule],
  controllers: [WageringController],
  providers: [
    ProcessWagerUseCase,
    IdempotencyService,
    { provide: WAGER_TRANSACTION_REPOSITORY, useClass: MikroOrmWagerTransactionRepository },
    { provide: IDEMPOTENCY_KEY_REPOSITORY, useClass: MikroOrmIdempotencyKeyRepository },
  ],
  exports: [WAGER_TRANSACTION_REPOSITORY],
})
export class WageringModule {}
```

> Note: this module imports `MessagingModule` (created in Task 8's follow-up,
> module wiring finalized in Task 16) because `ProcessWagerUseCase` depends
> on `OUTBOX_REPOSITORY`. If Task 16 has not run yet in your execution order,
> stub `MessagingModule` with just the `OUTBOX_REPOSITORY` provider — Task 16
> fills in the rest.

- [ ] **Step 5: Also update `WalletModule` to export the repository token**

```typescript
// src/wallet/wallet.module.ts
import { Module } from "@nestjs/common";
import { CreateWalletUseCase } from "./application/use-cases/create-wallet.use-case";
import { WALLET_REPOSITORY } from "./application/ports/wallet.repository";
import { MikroOrmWalletRepository } from "./infrastructure/persistence/repositories/wallet.repository";
import { WalletController } from "./infrastructure/http/wallet.controller";

@Module({
  controllers: [WalletController],
  providers: [
    CreateWalletUseCase,
    { provide: WALLET_REPOSITORY, useClass: MikroOrmWalletRepository },
  ],
  exports: [WALLET_REPOSITORY],
})
export class WalletModule {}
```

(unchanged from Task 9 — listed again here because `WageringModule` now
depends on it via `imports: [WalletModule]`.)

- [ ] **Step 6: Write the failing test**

```typescript
// test/wallet-reads.spec.ts
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { Money } from "../src/shared-kernel/money";
import { CreateWalletUseCase } from "../src/wallet/application/use-cases/create-wallet.use-case";
import { MikroOrmWalletRepository } from "../src/wallet/infrastructure/persistence/repositories/wallet.repository";
import { MikroOrmWagerTransactionRepository } from "../src/wagering/infrastructure/persistence/repositories/wager-transaction.repository";
import { WalletController } from "../src/wallet/infrastructure/http/wallet.controller";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env";

describe("WalletController — reads", () => {
  let db: TestDatabase;

  beforeAll(async () => {
    db = await startTestDatabase();
  }, 60_000);

  afterAll(async () => {
    await stopTestDatabase(db);
  });

  it("GET /wallets/:walletId returns the wallet", async () => {
    const em = db.orm.em.fork();
    const walletRepo = new MikroOrmWalletRepository(em);
    const createUseCase = new CreateWalletUseCase(em, walletRepo, new MikroOrmWagerTransactionRepository(em));
    const wallet = await createUseCase.execute({
      playerId: "player-read-1",
      currency: "BRL",
      initialBalance: Money.from({ amount: "50.00", currency: "BRL" }),
    });

    const readEm = db.orm.em.fork();
    const controller = new WalletController(createUseCase, new MikroOrmWalletRepository(readEm));
    const response = await controller.getWallet(wallet.id);
    expect(response.balance).toEqual({ amount: "50.00", currency: "BRL" });
  });

  it("GET /wallets/:walletId/ledger returns the OPENING entry", async () => {
    const em = db.orm.em.fork();
    const walletRepo = new MikroOrmWalletRepository(em);
    const createUseCase = new CreateWalletUseCase(em, walletRepo, new MikroOrmWagerTransactionRepository(em));
    const wallet = await createUseCase.execute({
      playerId: "player-read-2",
      currency: "BRL",
      initialBalance: Money.from({ amount: "50.00", currency: "BRL" }),
    });

    const readEm = db.orm.em.fork();
    const controller = new WalletController(createUseCase, new MikroOrmWalletRepository(readEm));
    const ledger = await controller.getLedger(wallet.id);
    expect(ledger.entries).toHaveLength(1);
    expect(ledger.entries[0].direction).toBe("CREDIT");
    expect(ledger.nextCursor).toBeNull();
  });
});
```

- [ ] **Step 7: Run the tests**

Run: `bun test test/wallet-reads.spec.ts`
Expected: PASS (2 tests).

- [ ] **Step 8: Commit**

```bash
git add src/wallet/infrastructure/http/ src/wallet/wallet.module.ts src/wagering/infrastructure/http/wagering.controller.ts src/wagering/wagering.module.ts test/wallet-reads.spec.ts
git commit -m "feat: add read endpoints for wallet, ledger and wager transactions"
```

---

## Task 13: ReconcileWalletUseCase + POST /wallets/:walletId/reconciliation

**Files:**
- Create: `src/wallet/application/use-cases/reconcile-wallet.use-case.ts`
- Create: `src/wallet/infrastructure/http/dto/reconciliation-response.dto.ts`
- Modify: `src/wallet/infrastructure/http/wallet.controller.ts`
- Modify: `src/wallet/wallet.module.ts`
- Test: `test/reconciliation.spec.ts`

**Interfaces:**
- Consumes: `WalletRepository.sumLedgerEntries` (Task 6, patched above).
- Produces:
  - `interface ReconciliationResult { walletId: string; storedBalance: Money; calculatedBalance: Money; difference: Money; consistent: boolean; checkedEntries: number }`
  - `class ReconcileWalletUseCase` with `execute(walletId: string): Promise<ReconciliationResult>`.
  - `POST /wallets/:walletId/reconciliation` → `ReconciliationResponseDto`.

- [ ] **Step 1: Write the use case**

```typescript
// src/wallet/application/use-cases/reconcile-wallet.use-case.ts
import { Inject, Injectable, Logger, NotFoundException } from "@nestjs/common";
import { Money } from "../../../shared-kernel/money";
import { WALLET_REPOSITORY, WalletRepository } from "../ports/wallet.repository";

export interface ReconciliationResult {
  walletId: string;
  storedBalance: Money;
  calculatedBalance: Money;
  difference: Money;
  consistent: boolean;
  checkedEntries: number;
}

@Injectable()
export class ReconcileWalletUseCase {
  private readonly logger = new Logger(ReconcileWalletUseCase.name);

  constructor(@Inject(WALLET_REPOSITORY) private readonly walletRepository: WalletRepository) {}

  async execute(walletId: string): Promise<ReconciliationResult> {
    const wallet = await this.walletRepository.findById(walletId);
    if (!wallet) {
      throw new NotFoundException(`Wallet ${walletId} not found`);
    }

    const { balance: calculatedBalance, count } = await this.walletRepository.sumLedgerEntries(
      walletId,
      wallet.currency,
    );
    const difference = wallet.balance.subtract(calculatedBalance);
    const consistent = difference.isZero();

    if (!consistent) {
      this.logger.error(
        `Reconciliation drift on wallet ${walletId}: stored=${wallet.balance.toString()} calculated=${calculatedBalance.toString()} difference=${difference.toString()}`,
      );
    }

    return {
      walletId,
      storedBalance: wallet.balance,
      calculatedBalance,
      difference,
      consistent,
      checkedEntries: count,
    };
  }
}
```

- [ ] **Step 2: Write the response DTO**

```typescript
// src/wallet/infrastructure/http/dto/reconciliation-response.dto.ts
import { ReconciliationResult } from "../../application/use-cases/reconcile-wallet.use-case";

export interface ReconciliationResponseDto {
  walletId: string;
  storedBalance: { amount: string; currency: string };
  calculatedBalance: { amount: string; currency: string };
  difference: { amount: string; currency: string };
  consistent: boolean;
  checkedEntries: number;
}

export function toReconciliationResponseDto(result: ReconciliationResult): ReconciliationResponseDto {
  return {
    walletId: result.walletId,
    storedBalance: result.storedBalance.toJSON(),
    calculatedBalance: result.calculatedBalance.toJSON(),
    difference: result.difference.toJSON(),
    consistent: result.consistent,
    checkedEntries: result.checkedEntries,
  };
}
```

- [ ] **Step 3: Add the endpoint to WalletController**

```typescript
// src/wallet/infrastructure/http/wallet.controller.ts
// (add alongside the existing constructor params and methods from Task 12)
import { Body, Controller, Get, Inject, NotFoundException, Param, Post, Query } from "@nestjs/common";
import { Money } from "../../../shared-kernel/money";
import { CreateWalletUseCase } from "../../application/use-cases/create-wallet.use-case";
import { ReconcileWalletUseCase } from "../../application/use-cases/reconcile-wallet.use-case";
import { WALLET_REPOSITORY, WalletRepository } from "../../application/ports/wallet.repository";
import { CreateWalletDto } from "./dto/create-wallet.dto";
import { toWalletResponseDto, WalletResponseDto } from "./dto/wallet-response.dto";
import { toLedgerResponseDto, LedgerResponseDto } from "./dto/ledger-response.dto";
import { toReconciliationResponseDto, ReconciliationResponseDto } from "./dto/reconciliation-response.dto";

@Controller("wallets")
export class WalletController {
  constructor(
    private readonly createWalletUseCase: CreateWalletUseCase,
    private readonly reconcileWalletUseCase: ReconcileWalletUseCase,
    @Inject(WALLET_REPOSITORY) private readonly walletRepository: WalletRepository,
  ) {}

  @Post()
  async create(@Body() dto: CreateWalletDto): Promise<WalletResponseDto> {
    const initialBalance = Money.from({ amount: dto.initialBalance ?? "0.00", currency: dto.currency });
    const wallet = await this.createWalletUseCase.execute({
      playerId: dto.playerId,
      currency: dto.currency,
      initialBalance,
    });
    return toWalletResponseDto(wallet);
  }

  @Get(":walletId")
  async getWallet(@Param("walletId") walletId: string): Promise<WalletResponseDto> {
    const wallet = await this.walletRepository.findById(walletId);
    if (!wallet) {
      throw new NotFoundException(`Wallet ${walletId} not found`);
    }
    return toWalletResponseDto(wallet);
  }

  @Get(":walletId/ledger")
  async getLedger(
    @Param("walletId") walletId: string,
    @Query("after") after?: string,
    @Query("limit") limit?: string,
  ): Promise<LedgerResponseDto> {
    const parsedLimit = limit ? Math.min(Math.max(parseInt(limit, 10), 1), 100) : 20;
    const entries = await this.walletRepository.listLedgerEntries(walletId, { after, limit: parsedLimit });
    return toLedgerResponseDto(entries, parsedLimit);
  }

  @Post(":walletId/reconciliation")
  async reconcile(@Param("walletId") walletId: string): Promise<ReconciliationResponseDto> {
    const result = await this.reconcileWalletUseCase.execute(walletId);
    return toReconciliationResponseDto(result);
  }
}
```

- [ ] **Step 4: Update WalletModule**

```typescript
// src/wallet/wallet.module.ts
import { Module } from "@nestjs/common";
import { CreateWalletUseCase } from "./application/use-cases/create-wallet.use-case";
import { ReconcileWalletUseCase } from "./application/use-cases/reconcile-wallet.use-case";
import { WALLET_REPOSITORY } from "./application/ports/wallet.repository";
import { MikroOrmWalletRepository } from "./infrastructure/persistence/repositories/wallet.repository";
import { WalletController } from "./infrastructure/http/wallet.controller";

@Module({
  controllers: [WalletController],
  providers: [
    CreateWalletUseCase,
    ReconcileWalletUseCase,
    { provide: WALLET_REPOSITORY, useClass: MikroOrmWalletRepository },
  ],
  exports: [WALLET_REPOSITORY],
})
export class WalletModule {}
```

- [ ] **Step 5: Write the failing test**

```typescript
// test/reconciliation.spec.ts
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { Money } from "../src/shared-kernel/money";
import { CreateWalletUseCase } from "../src/wallet/application/use-cases/create-wallet.use-case";
import { ReconcileWalletUseCase } from "../src/wallet/application/use-cases/reconcile-wallet.use-case";
import { MikroOrmWalletRepository } from "../src/wallet/infrastructure/persistence/repositories/wallet.repository";
import { MikroOrmWagerTransactionRepository } from "../src/wagering/infrastructure/persistence/repositories/wager-transaction.repository";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env";

describe("ReconcileWalletUseCase", () => {
  let db: TestDatabase;

  beforeAll(async () => {
    db = await startTestDatabase();
  }, 60_000);

  afterAll(async () => {
    await stopTestDatabase(db);
  });

  it("reports consistent: true when balance matches the ledger", async () => {
    const em = db.orm.em.fork();
    const wallet = await new CreateWalletUseCase(
      em,
      new MikroOrmWalletRepository(em),
      new MikroOrmWagerTransactionRepository(em),
    ).execute({ playerId: "player-recon-1", currency: "BRL", initialBalance: Money.from({ amount: "80.00", currency: "BRL" }) });

    const reconcileEm = db.orm.em.fork();
    const result = await new ReconcileWalletUseCase(new MikroOrmWalletRepository(reconcileEm)).execute(wallet.id);

    expect(result.consistent).toBe(true);
    expect(result.storedBalance.toString()).toBe("80.00");
    expect(result.calculatedBalance.toString()).toBe("80.00");
    expect(result.difference.isZero()).toBe(true);
    expect(result.checkedEntries).toBe(1);
  });

  it("reports consistent: false and the exact drift when the stored balance was corrupted", async () => {
    const em = db.orm.em.fork();
    const wallet = await new CreateWalletUseCase(
      em,
      new MikroOrmWalletRepository(em),
      new MikroOrmWagerTransactionRepository(em),
    ).execute({ playerId: "player-recon-2", currency: "BRL", initialBalance: Money.from({ amount: "80.00", currency: "BRL" }) });

    // Simulate a bug that corrupted the materialized balance without touching the ledger.
    const corruptEm = db.orm.em.fork();
    await corruptEm.getConnection().execute(`update wallets set balance = '999.00' where id = $1`, [wallet.id]);

    const reconcileEm = db.orm.em.fork();
    const result = await new ReconcileWalletUseCase(new MikroOrmWalletRepository(reconcileEm)).execute(wallet.id);

    expect(result.consistent).toBe(false);
    expect(result.storedBalance.toString()).toBe("999.00");
    expect(result.calculatedBalance.toString()).toBe("80.00");
    expect(result.difference.toString()).toBe("919.00");
  });
});
```

- [ ] **Step 6: Run the tests**

Run: `bun test test/reconciliation.spec.ts`
Expected: PASS (2 tests).

- [ ] **Step 7: Commit**

```bash
git add src/wallet/application/use-cases/reconcile-wallet.use-case.ts src/wallet/infrastructure/http/ src/wallet/wallet.module.ts test/reconciliation.spec.ts
git commit -m "feat: add reconciliation use case and POST /wallets/:walletId/reconciliation"
```

---

## Task 14: SQS client provider + LocalStack queue bootstrap

**Files:**
- Create: `src/messaging/infrastructure/sqs/sqs-client.provider.ts`
- Create: `src/messaging/infrastructure/sqs/bootstrap-queues.ts`
- Modify: `package.json` (add `"bootstrap:queues"` script)
- Test: `test/bootstrap-queues.spec.ts`

**Interfaces:**
- Consumes: `@aws-sdk/client-sqs` (already installed), env vars `AWS_REGION`,
  `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (already
  in `.env`/`.env.example`).
- Produces:
  - `createSqsClient(): SQSClient`
  - `const SQS_CLIENT: symbol`
  - `bootstrapQueues(client: SQSClient): Promise<{ mainQueueUrl: string; dlqUrl: string }>` — creates
    `wager-transactions-dlq.fifo` then `wager-transactions.fifo` with a
    redrive policy pointing at the DLQ (`maxReceiveCount: 5`).

- [ ] **Step 1: Write the SQS client provider**

```typescript
// src/messaging/infrastructure/sqs/sqs-client.provider.ts
import { SQSClient } from "@aws-sdk/client-sqs";

export const SQS_CLIENT = Symbol("SQS_CLIENT");

export function createSqsClient(): SQSClient {
  return new SQSClient({
    region: process.env.AWS_REGION ?? "us-east-1",
    endpoint: process.env.AWS_ENDPOINT_URL,
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? "test",
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? "test",
    },
  });
}
```

- [ ] **Step 2: Write the bootstrap script**

```typescript
// src/messaging/infrastructure/sqs/bootstrap-queues.ts
import {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { createSqsClient } from "./sqs-client.provider";

export async function bootstrapQueues(
  client: SQSClient,
): Promise<{ mainQueueUrl: string; dlqUrl: string }> {
  const dlq = await client.send(
    new CreateQueueCommand({
      QueueName: "wager-transactions-dlq.fifo",
      Attributes: { FifoQueue: "true", ContentBasedDeduplication: "false" },
    }),
  );
  const dlqUrl = dlq.QueueUrl!;

  const dlqAttrs = await client.send(
    new GetQueueAttributesCommand({ QueueUrl: dlqUrl, AttributeNames: ["QueueArn"] }),
  );
  const dlqArn = dlqAttrs.Attributes!.QueueArn!;

  const main = await client.send(
    new CreateQueueCommand({
      QueueName: "wager-transactions.fifo",
      Attributes: {
        FifoQueue: "true",
        ContentBasedDeduplication: "false",
        VisibilityTimeout: "30",
        RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlqArn, maxReceiveCount: "5" }),
      },
    }),
  );

  return { mainQueueUrl: main.QueueUrl!, dlqUrl };
}

if (import.meta.main) {
  bootstrapQueues(createSqsClient())
    .then(({ mainQueueUrl, dlqUrl }) => {
      console.log(`Main queue: ${mainQueueUrl}`);
      console.log(`DLQ: ${dlqUrl}`);
    })
    .catch((err) => {
      console.error(err);
      process.exit(1);
    });
}
```

- [ ] **Step 3: Add the package.json script**

```json
"bootstrap:queues": "bun run src/messaging/infrastructure/sqs/bootstrap-queues.ts"
```

- [ ] **Step 4: Write the failing test**

```typescript
// test/bootstrap-queues.spec.ts
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalstackContainer, StartedLocalStackContainer } from "@testcontainers/localstack";
import { GetQueueAttributesCommand, SQSClient } from "@aws-sdk/client-sqs";
import { bootstrapQueues } from "../src/messaging/infrastructure/sqs/bootstrap-queues";

describe("bootstrapQueues", () => {
  let container: StartedLocalStackContainer;
  let client: SQSClient;

  beforeAll(async () => {
    container = await new LocalstackContainer("localstack/localstack:3").withServices(["sqs"]).start();
    client = new SQSClient({
      region: "us-east-1",
      endpoint: container.getConnectionUri(),
      credentials: { accessKeyId: "test", secretAccessKey: "test" },
    });
  }, 60_000);

  afterAll(async () => {
    await container.stop();
  });

  it("creates the main FIFO queue with a redrive policy pointing at the DLQ", async () => {
    const { mainQueueUrl, dlqUrl } = await bootstrapQueues(client);
    expect(mainQueueUrl).toContain("wager-transactions.fifo");
    expect(dlqUrl).toContain("wager-transactions-dlq.fifo");

    const attrs = await client.send(
      new GetQueueAttributesCommand({ QueueUrl: mainQueueUrl, AttributeNames: ["RedrivePolicy"] }),
    );
    const redrivePolicy = JSON.parse(attrs.Attributes!.RedrivePolicy!);
    expect(redrivePolicy.maxReceiveCount).toBe("5");
  });
});
```

- [ ] **Step 5: Run the test**

Run: `bun test test/bootstrap-queues.spec.ts`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/messaging/infrastructure/sqs/sqs-client.provider.ts src/messaging/infrastructure/sqs/bootstrap-queues.ts package.json test/bootstrap-queues.spec.ts
git commit -m "feat: add SQS client provider and LocalStack queue bootstrap script"
```

---

## Task 15: SQS consumer — inbox dedup, same use case as HTTP, graceful shutdown

**Files:**
- Create: `src/messaging/infrastructure/sqs/wager-transaction.consumer.ts`
- Test: `test/inbox-redelivery.spec.ts`

**Interfaces:**
- Consumes: `SQS_CLIENT`/`createSqsClient` (Task 14), `MikroOrmInboxRepository`
  (Task 8), `ProcessWagerUseCase` (Task 10), `MikroOrmWalletRepository`
  (Task 6), `MikroOrmWagerTransactionRepository` (Task 7),
  `MikroOrmOutboxRepository` (Task 8), `bootstrapQueues` (Task 14),
  `computePayloadHash` (Task 3).
- Produces:
  - `interface WagerTransactionEnvelope` (message body shape).
  - `class WagerTransactionConsumer implements OnModuleInit, OnModuleDestroy`
    with `handleMessage(message: Message): Promise<void>` (public — exercised
    directly by the test without waiting on the poll loop's timing).

- [ ] **Step 1: Write the consumer**

```typescript
// src/messaging/infrastructure/sqs/wager-transaction.consumer.ts
import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import { MikroORM } from "@mikro-orm/postgresql";
import {
  DeleteMessageCommand,
  Message,
  ReceiveMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { Money } from "../../../shared-kernel/money";
import { WagerKind } from "../../../wagering/domain/wager-transaction";
import { computePayloadHash } from "../../../wagering/domain/payload-hash";
import { ProcessWagerUseCase } from "../../../wagering/application/use-cases/process-wager.use-case";
import { MikroOrmWalletRepository } from "../../../wallet/infrastructure/persistence/repositories/wallet.repository";
import { MikroOrmWagerTransactionRepository } from "../../../wagering/infrastructure/persistence/repositories/wager-transaction.repository";
import { MikroOrmOutboxRepository } from "../persistence/repositories/outbox.repository";
import { MikroOrmInboxRepository } from "../persistence/repositories/inbox.repository";
import { SQS_CLIENT } from "./sqs-client.provider";

export interface WagerTransactionEnvelope {
  messageId: string;
  type: string;
  occurredAt: string;
  data: {
    providerId: string;
    externalTransactionId: string;
    idempotencyKey: string;
    playerId: string;
    walletId: string;
    roundId?: string;
    gameId?: string;
    kind: WagerKind;
    money: { amount: string; currency: string };
    referenceExternalTransactionId?: string;
  };
}

@Injectable()
export class WagerTransactionConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(WagerTransactionConsumer.name);
  private readonly consumerName = "wager-transaction-consumer";
  private readonly queueUrl = process.env.SQS_QUEUE_URL ?? "";
  private stopped = false;
  private inFlight = 0;
  private pollingPromise: Promise<void> | null = null;

  constructor(
    @Inject(SQS_CLIENT) private readonly sqsClient: SQSClient,
    private readonly orm: MikroORM,
  ) {}

  onModuleInit(): void {
    if (!this.queueUrl) {
      this.logger.warn("SQS_QUEUE_URL not set — consumer will not start polling");
      return;
    }
    this.pollingPromise = this.pollLoop();
  }

  async onModuleDestroy(): Promise<void> {
    this.stopped = true;
    await this.pollingPromise;
    while (this.inFlight > 0) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  }

  private async pollLoop(): Promise<void> {
    while (!this.stopped) {
      const result = await this.sqsClient.send(
        new ReceiveMessageCommand({ QueueUrl: this.queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 5 }),
      );
      for (const message of result.Messages ?? []) {
        if (this.stopped) break;
        this.inFlight += 1;
        try {
          await this.handleMessage(message);
        } catch (err) {
          this.logger.error(`failed to process message ${message.MessageId}`, err as Error);
        } finally {
          this.inFlight -= 1;
        }
      }
    }
  }

  async handleMessage(message: Message): Promise<void> {
    const envelope = JSON.parse(message.Body!) as WagerTransactionEnvelope;
    const em = this.orm.em.fork();

    await em.transactional(async (tx) => {
      const inboxRepository = new MikroOrmInboxRepository(tx);
      const alreadyProcessed = await inboxRepository.exists(this.consumerName, message.MessageId!);
      if (alreadyProcessed) {
        this.logger.debug(`message ${message.MessageId} already processed — skipping (dedup)`);
        return;
      }
      await inboxRepository.markProcessed(this.consumerName, message.MessageId!);

      const useCase = new ProcessWagerUseCase(
        tx,
        new MikroOrmWalletRepository(tx),
        new MikroOrmWagerTransactionRepository(tx),
        new MikroOrmOutboxRepository(tx, this.orm),
      );
      await useCase.execute({
        externalTransactionId: envelope.data.externalTransactionId,
        providerId: envelope.data.providerId,
        idempotencyKey: envelope.data.idempotencyKey,
        payloadHash: computePayloadHash(envelope.data as unknown as Record<string, unknown>),
        kind: envelope.data.kind,
        walletId: envelope.data.walletId,
        amount: Money.from(envelope.data.money),
        referenceExternalTransactionId: envelope.data.referenceExternalTransactionId ?? null,
      });
    });

    try {
      await this.sqsClient.send(
        new DeleteMessageCommand({ QueueUrl: this.queueUrl, ReceiptHandle: message.ReceiptHandle! }),
      );
    } catch (err) {
      // A redundant delete (already removed, or a stale receipt handle after redelivery)
      // is not a failure — the message is gone either way.
      this.logger.debug(`ack for message ${message.MessageId} skipped: ${(err as Error).message}`);
    }
  }
}
```

- [ ] **Step 2: Write the failing test**

```typescript
// test/inbox-redelivery.spec.ts
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalstackContainer, StartedLocalStackContainer } from "@testcontainers/localstack";
import { ReceiveMessageCommand, SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { Money } from "../src/shared-kernel/money";
import { CreateWalletUseCase } from "../src/wallet/application/use-cases/create-wallet.use-case";
import { MikroOrmWalletRepository } from "../src/wallet/infrastructure/persistence/repositories/wallet.repository";
import { MikroOrmWagerTransactionRepository } from "../src/wagering/infrastructure/persistence/repositories/wager-transaction.repository";
import { bootstrapQueues } from "../src/messaging/infrastructure/sqs/bootstrap-queues";
import { WagerTransactionConsumer } from "../src/messaging/infrastructure/sqs/wager-transaction.consumer";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env";

describe("WagerTransactionConsumer — inbox dedup on redelivery", () => {
  let db: TestDatabase;
  let localstack: StartedLocalStackContainer;
  let sqsClient: SQSClient;
  let queueUrl: string;

  beforeAll(async () => {
    db = await startTestDatabase();
    localstack = await new LocalstackContainer("localstack/localstack:3").withServices(["sqs"]).start();
    sqsClient = new SQSClient({
      region: "us-east-1",
      endpoint: localstack.getConnectionUri(),
      credentials: { accessKeyId: "test", secretAccessKey: "test" },
    });
    const queues = await bootstrapQueues(sqsClient);
    queueUrl = queues.mainQueueUrl;
  }, 90_000);

  afterAll(async () => {
    await stopTestDatabase(db);
    await localstack.stop();
  });

  it("processes a redelivered message exactly once", async () => {
    const em = db.orm.em.fork();
    const wallet = await new CreateWalletUseCase(
      em,
      new MikroOrmWalletRepository(em),
      new MikroOrmWagerTransactionRepository(em),
    ).execute({ playerId: "player-inbox-1", currency: "BRL", initialBalance: Money.from({ amount: "100.00", currency: "BRL" }) });

    const envelope = {
      messageId: "envelope-1",
      type: "WagerTransactionRequested",
      occurredAt: new Date().toISOString(),
      data: {
        providerId: "provider-sqs",
        externalTransactionId: "sqs-bet-1",
        idempotencyKey: "idem-sqs-1",
        playerId: "player-inbox-1",
        walletId: wallet.id,
        kind: "BET",
        money: { amount: "40.00", currency: "BRL" },
      },
    };

    await sqsClient.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify(envelope),
        MessageGroupId: wallet.id,
        MessageDeduplicationId: "dedup-1",
      }),
    );
    const received = await sqsClient.send(
      new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 5 }),
    );
    const message = received.Messages![0];

    const consumer = new WagerTransactionConsumer(sqsClient, db.orm);
    // Simulate the queue redelivering the same message before the first ack lands.
    await consumer.handleMessage(message);
    await consumer.handleMessage(message);

    const verifyEm = db.orm.em.fork();
    const finalWallet = await new MikroOrmWalletRepository(verifyEm).findById(wallet.id);
    expect(finalWallet?.balance.toString()).toBe("60.00"); // debited exactly once, not twice
  }, 30_000);
});
```

- [ ] **Step 3: Run the test**

Run: `bun test test/inbox-redelivery.spec.ts`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/messaging/infrastructure/sqs/wager-transaction.consumer.ts test/inbox-redelivery.spec.ts
git commit -m "feat: add SQS consumer with inbox dedup and graceful shutdown"
```

---

## Task 16: Outbox publisher worker + MessagingModule

**Files:**
- Create: `src/messaging/infrastructure/sqs/outbox-publisher.worker.ts`
- Create: `src/messaging/messaging.module.ts`
- Test: `test/outbox-publisher-worker.spec.ts`

**Interfaces:**
- Consumes: `MikroOrmOutboxRepository` (Task 8), `SQS_CLIENT`/`createSqsClient`
  (Task 14), `bootstrapQueues` (Task 14).
- Produces:
  - `class OutboxPublisherWorker implements OnModuleInit, OnModuleDestroy`.
  - `class MessagingModule` — the final version of the module that Task 12's
    `WageringModule` imports for `OUTBOX_REPOSITORY`.

- [ ] **Step 1: Write the publisher worker**

```typescript
// src/messaging/infrastructure/sqs/outbox-publisher.worker.ts
import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import { MikroORM } from "@mikro-orm/postgresql";
import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { MikroOrmOutboxRepository } from "../persistence/repositories/outbox.repository";
import { SQS_CLIENT } from "./sqs-client.provider";

const POLL_INTERVAL_MS = 2000;
const BATCH_SIZE = 10;

@Injectable()
export class OutboxPublisherWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxPublisherWorker.name);
  private readonly queueUrl = process.env.SQS_QUEUE_URL ?? "";
  private timer: ReturnType<typeof setInterval> | null = null;
  private ticking = false;

  constructor(
    @Inject(SQS_CLIENT) private readonly sqsClient: SQSClient,
    private readonly orm: MikroORM,
  ) {}

  onModuleInit(): void {
    if (!this.queueUrl) {
      this.logger.warn("SQS_QUEUE_URL not set — outbox publisher disabled");
      return;
    }
    this.timer = setInterval(() => {
      void this.tick();
    }, POLL_INTERVAL_MS);
  }

  onModuleDestroy(): void {
    if (this.timer) {
      clearInterval(this.timer);
    }
  }

  async tick(): Promise<number> {
    if (this.ticking) {
      return 0;
    }
    this.ticking = true;
    try {
      const repository = new MikroOrmOutboxRepository(this.orm.em.fork(), this.orm);
      return await repository.processDueBatch(BATCH_SIZE, async (message) => {
        try {
          await this.sqsClient.send(
            new SendMessageCommand({
              QueueUrl: this.queueUrl,
              MessageBody: JSON.stringify({
                eventType: message.eventType,
                aggregateId: message.aggregateId,
                occurredAt: message.occurredAt,
                payload: message.payload,
              }),
              MessageGroupId: message.aggregateId,
              MessageDeduplicationId: message.id,
            }),
          );
          return true;
        } catch (err) {
          this.logger.error(`failed to publish outbox message ${message.id}`, err as Error);
          return false;
        }
      });
    } finally {
      this.ticking = false;
    }
  }
}
```

- [ ] **Step 2: Write the MessagingModule**

```typescript
// src/messaging/messaging.module.ts
import { Module } from "@nestjs/common";
import { SQS_CLIENT, createSqsClient } from "./infrastructure/sqs/sqs-client.provider";
import { OUTBOX_REPOSITORY } from "./application/ports/outbox.repository";
import { INBOX_REPOSITORY } from "./application/ports/inbox.repository";
import { MikroOrmOutboxRepository } from "./infrastructure/persistence/repositories/outbox.repository";
import { MikroOrmInboxRepository } from "./infrastructure/persistence/repositories/inbox.repository";
import { WagerTransactionConsumer } from "./infrastructure/sqs/wager-transaction.consumer";
import { OutboxPublisherWorker } from "./infrastructure/sqs/outbox-publisher.worker";

@Module({
  providers: [
    { provide: SQS_CLIENT, useFactory: createSqsClient },
    { provide: OUTBOX_REPOSITORY, useClass: MikroOrmOutboxRepository },
    { provide: INBOX_REPOSITORY, useClass: MikroOrmInboxRepository },
    WagerTransactionConsumer,
    OutboxPublisherWorker,
  ],
  exports: [OUTBOX_REPOSITORY, SQS_CLIENT],
})
export class MessagingModule {}
```

- [ ] **Step 3: Write the failing test**

```typescript
// test/outbox-publisher-worker.spec.ts
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { LocalstackContainer, StartedLocalStackContainer } from "@testcontainers/localstack";
import { ReceiveMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { MikroOrmOutboxRepository } from "../src/messaging/infrastructure/persistence/repositories/outbox.repository";
import { OutboxMessageEntity } from "../src/messaging/infrastructure/persistence/entities/outbox-message.entity";
import { bootstrapQueues } from "../src/messaging/infrastructure/sqs/bootstrap-queues";
import { OutboxPublisherWorker } from "../src/messaging/infrastructure/sqs/outbox-publisher.worker";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env";

describe("OutboxPublisherWorker", () => {
  let db: TestDatabase;
  let localstack: StartedLocalStackContainer;
  let sqsClient: SQSClient;
  let queueUrl: string;

  beforeAll(async () => {
    db = await startTestDatabase();
    localstack = await new LocalstackContainer("localstack/localstack:3").withServices(["sqs"]).start();
    sqsClient = new SQSClient({
      region: "us-east-1",
      endpoint: localstack.getConnectionUri(),
      credentials: { accessKeyId: "test", secretAccessKey: "test" },
    });
    ({ mainQueueUrl: queueUrl } = await bootstrapQueues(sqsClient));
    process.env.SQS_QUEUE_URL = queueUrl;
  }, 90_000);

  afterAll(async () => {
    await stopTestDatabase(db);
    await localstack.stop();
  });

  it("publishes a pending outbox row to SQS and marks it published", async () => {
    const setupEm = db.orm.em.fork();
    const outboxRepo = new MikroOrmOutboxRepository(setupEm, db.orm);
    await outboxRepo.append({
      id: "88888888-8888-8888-8888-888888888888",
      aggregateId: "wallet-pub-1",
      eventType: "WagerProcessed",
      payload: { hello: "world" },
      occurredAt: new Date(),
    });
    await setupEm.flush();

    const worker = new OutboxPublisherWorker(sqsClient, db.orm);
    const processed = await worker.tick();
    expect(processed).toBe(1);

    const verifyEm = db.orm.em.fork();
    const row = await verifyEm.findOne(OutboxMessageEntity, { id: "88888888-8888-8888-8888-888888888888" });
    expect(row?.publishedAt).not.toBeNull();

    const received = await sqsClient.send(
      new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 5 }),
    );
    expect(received.Messages).toHaveLength(1);
    const body = JSON.parse(received.Messages![0].Body!);
    expect(body.aggregateId).toBe("wallet-pub-1");
  }, 30_000);
});
```

- [ ] **Step 4: Run the test**

Run: `bun test test/outbox-publisher-worker.spec.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/messaging/infrastructure/sqs/outbox-publisher.worker.ts src/messaging/messaging.module.ts test/outbox-publisher-worker.spec.ts
git commit -m "feat: add outbox publisher worker and finalize MessagingModule"
```

---

## Task 17: HealthModule (/health/live, /health/ready)

**Files:**
- Create: `src/health/health.controller.ts`
- Create: `src/health/health.module.ts`
- Test: `test/health.spec.ts`

**Interfaces:**
- Consumes: `EntityManager` (MikroORM), `SQS_CLIENT` (Task 14/16).
- Produces:
  - `GET /health/live` → `{ status: "ok" }` always (process alive).
  - `GET /health/ready` → `{ database: "ok" | "error"; sqs: "ok" | "error" }`,
    throws `ServiceUnavailableException` (503) if either check fails. Both
    routes are unauthenticated per the spec.

- [ ] **Step 1: Write the controller**

```typescript
// src/health/health.controller.ts
import { Controller, Get, Inject, ServiceUnavailableException } from "@nestjs/common";
import { EntityManager } from "@mikro-orm/postgresql";
import { ListQueuesCommand, SQSClient } from "@aws-sdk/client-sqs";
import { SQS_CLIENT } from "../messaging/infrastructure/sqs/sqs-client.provider";

type CheckStatus = "ok" | "error";

@Controller("health")
export class HealthController {
  constructor(
    private readonly em: EntityManager,
    @Inject(SQS_CLIENT) private readonly sqsClient: SQSClient,
  ) {}

  @Get("live")
  live(): { status: "ok" } {
    return { status: "ok" };
  }

  @Get("ready")
  async ready(): Promise<{ database: CheckStatus; sqs: CheckStatus }> {
    const checks: { database: CheckStatus; sqs: CheckStatus } = { database: "ok", sqs: "ok" };

    try {
      await this.em.getConnection().execute("select 1");
    } catch {
      checks.database = "error";
    }

    try {
      await this.sqsClient.send(new ListQueuesCommand({}));
    } catch {
      checks.sqs = "error";
    }

    if (checks.database === "error" || checks.sqs === "error") {
      throw new ServiceUnavailableException(checks);
    }
    return checks;
  }
}
```

- [ ] **Step 2: Write the module**

```typescript
// src/health/health.module.ts
import { Module } from "@nestjs/common";
import { MessagingModule } from "../messaging/messaging.module";
import { HealthController } from "./health.controller";

@Module({
  imports: [MessagingModule],
  controllers: [HealthController],
})
export class HealthModule {}
```

- [ ] **Step 3: Write the failing test**

```typescript
// test/health.spec.ts
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { SQSClient } from "@aws-sdk/client-sqs";
import { HealthController } from "../src/health/health.controller";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env";

describe("HealthController", () => {
  let db: TestDatabase;

  beforeAll(async () => {
    db = await startTestDatabase();
  }, 60_000);

  afterAll(async () => {
    await stopTestDatabase(db);
  });

  it("GET /health/live always returns ok", () => {
    const controller = new HealthController(db.orm.em.fork(), new SQSClient({ region: "us-east-1" }));
    expect(controller.live()).toEqual({ status: "ok" });
  });

  it("GET /health/ready returns ok when Postgres and SQS are both reachable", async () => {
    const sqsClient = new SQSClient({
      region: "us-east-1",
      endpoint: process.env.AWS_ENDPOINT_URL ?? "http://localhost:4566",
      credentials: { accessKeyId: "test", secretAccessKey: "test" },
    });
    const controller = new HealthController(db.orm.em.fork(), sqsClient);
    await expect(controller.ready()).resolves.toEqual({ database: "ok", sqs: "ok" });
  });

  it("GET /health/ready reports sqs: error and throws 503 when SQS is unreachable", async () => {
    const unreachableSqsClient = new SQSClient({
      region: "us-east-1",
      endpoint: "http://localhost:1", // nothing listens here
      credentials: { accessKeyId: "test", secretAccessKey: "test" },
      maxAttempts: 1,
    });
    const controller = new HealthController(db.orm.em.fork(), unreachableSqsClient);
    await expect(controller.ready()).rejects.toThrow();
  });
});
```

> Note: the second test requires LocalStack reachable at `AWS_ENDPOINT_URL`
> (default `http://localhost:4566`) — run `docker compose up -d localstack`
> before running this test file locally.

- [ ] **Step 4: Run the test**

Run: `docker compose up -d localstack && bun test test/health.spec.ts`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/health/ test/health.spec.ts
git commit -m "feat: add /health/live and /health/ready endpoints"
```

---

## Task 18: Wire AppModule — full boot, end-to-end smoke test

**Files:**
- Modify: `src/app.module.ts`
- Test: `test/e2e-smoke.spec.ts`

**Interfaces:**
- Consumes: every module built so far (`WalletModule`, `WageringModule`,
  `MessagingModule`, `HealthModule`).
- Produces: a fully wired Nest application — this is the first task where
  the whole HTTP surface is reachable together.

- [ ] **Step 1: Update AppModule**

```typescript
// src/app.module.ts
import { Module } from "@nestjs/common";
import { MikroOrmModule } from "@mikro-orm/nestjs";
import { ConfigModule } from "@nestjs/config";
import mikroOrmConfig from "../mikro-orm.config";
import { WalletModule } from "./wallet/wallet.module";
import { WageringModule } from "./wagering/wagering.module";
import { MessagingModule } from "./messaging/messaging.module";
import { HealthModule } from "./health/health.module";

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    MikroOrmModule.forRoot(mikroOrmConfig),
    WalletModule,
    WageringModule,
    MessagingModule,
    HealthModule,
  ],
})
export class AppModule {}
```

- [ ] **Step 2: Write the failing end-to-end test**

This test exercises the docker-compose stack directly (`docker compose up -d`
before running it) rather than Testcontainers — it is the one test that
proves the whole app boots and serves real HTTP end to end, mirroring how a
developer actually runs it.

```typescript
// test/e2e-smoke.spec.ts
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { AppModule } from "../src/app.module";

describe("End-to-end smoke test (requires `docker compose up -d`)", () => {
  let app: INestApplication;

  beforeAll(async () => {
    const moduleRef: TestingModule = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleRef.createNestApplication();
    app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
    await app.init();
  }, 30_000);

  afterAll(async () => {
    await app.close();
  });

  it("GET /health/live and /health/ready respond", async () => {
    await request(app.getHttpServer()).get("/health/live").expect(200, { status: "ok" });
    await request(app.getHttpServer()).get("/health/ready").expect(200, { database: "ok", sqs: "ok" });
  });

  it("creates a wallet, submits a BET over HTTP, and reads it back", async () => {
    const playerId = `player-e2e-${Date.now()}`;
    const createResponse = await request(app.getHttpServer())
      .post("/wallets")
      .send({ playerId, currency: "BRL", initialBalance: "100.00" })
      .expect(201);

    const walletId = createResponse.body.id;

    const betResponse = await request(app.getHttpServer())
      .post("/wagering/transactions")
      .set("Idempotency-Key", `idem-e2e-${walletId}`)
      .send({
        externalTransactionId: `bet-e2e-${walletId}`,
        providerId: "provider-e2e",
        walletId,
        kind: "BET",
        amount: "40.00",
        currency: "BRL",
      })
      .expect(201);

    expect(betResponse.body.status).toBe("PROCESSED");
    expect(betResponse.body.balance).toEqual({ amount: "60.00", currency: "BRL" });

    const walletResponse = await request(app.getHttpServer()).get(`/wallets/${walletId}`).expect(200);
    expect(walletResponse.body.balance).toEqual({ amount: "60.00", currency: "BRL" });
  });
});
```

- [ ] **Step 3: Run the test**

Run:
```bash
docker compose up -d postgres localstack
bunx mikro-orm migration:up
bunx mikro-orm-cli 2>/dev/null; bun run src/messaging/infrastructure/sqs/bootstrap-queues.ts
bun test test/e2e-smoke.spec.ts
```
Expected: PASS (2 tests).

- [ ] **Step 4: Commit**

```bash
git add src/app.module.ts test/e2e-smoke.spec.ts
git commit -m "feat: wire AppModule, add end-to-end smoke test"
```

---

## Task 19: Concurrency tests — the mandatory hot-wallet scenario, ≥3 instances, 50-way parallel idempotency

**Files:**
- Test: `test/concurrency-hot-wallet.spec.ts`

**Interfaces:**
- Consumes: `ProcessWagerUseCase` (Task 10, with the race-handling fix),
  `CreateWalletUseCase` (Task 9), `MikroOrmWalletRepository` (Task 6),
  `MikroOrmWagerTransactionRepository` (Task 7), `MikroOrmOutboxRepository`
  (Task 8).
- Produces: no new production code — this task is pure verification of
  behavior already built. If any assertion fails, the bug is in Task 10.

> **On "≥3 instances":** the spec requires correctness across 3+ instances.
> This suite proves it with 3+ concurrent `ProcessWagerUseCase` instances,
> each built from its own independent `EntityManager` fork (its own DB
> connection from the pool) — genuine concurrent SQL transactions racing
> against Postgres, not sequential mocks. It does not spin 3 separate OS
> processes/containers. The correctness property being tested (row-level
> locking, unique constraints) lives entirely in Postgres, which cannot
> distinguish "3 forks in one Bun process" from "3 separate containers" —
> both send concurrent transactions over separate connections. This
> simplification is documented as a deliberate scope decision in
> `ARCHITECTURE.md` (Task 20).

- [ ] **Step 1: Write the failing tests**

```typescript
// test/concurrency-hot-wallet.spec.ts
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { Money } from "../src/shared-kernel/money";
import { FailureCode } from "../src/shared-kernel/failure-code";
import { CreateWalletUseCase } from "../src/wallet/application/use-cases/create-wallet.use-case";
import { MikroOrmWalletRepository } from "../src/wallet/infrastructure/persistence/repositories/wallet.repository";
import { MikroOrmWagerTransactionRepository } from "../src/wagering/infrastructure/persistence/repositories/wager-transaction.repository";
import { MikroOrmOutboxRepository } from "../src/messaging/infrastructure/persistence/repositories/outbox.repository";
import { ProcessWagerUseCase, ProcessWagerInput } from "../src/wagering/application/use-cases/process-wager.use-case";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env";

let db: TestDatabase;

function freshUseCase(): ProcessWagerUseCase {
  const em = db.orm.em.fork();
  return new ProcessWagerUseCase(
    em,
    new MikroOrmWalletRepository(em),
    new MikroOrmWagerTransactionRepository(em),
    new MikroOrmOutboxRepository(em, db.orm),
  );
}

async function seedWallet(playerId: string, initial: string): Promise<string> {
  const em = db.orm.em.fork();
  const wallet = await new CreateWalletUseCase(
    em,
    new MikroOrmWalletRepository(em),
    new MikroOrmWagerTransactionRepository(em),
  ).execute({ playerId, currency: "BRL", initialBalance: Money.from({ amount: initial, currency: "BRL" }) });
  return wallet.id;
}

describe("Concurrency — hot wallet, multi-instance, mass duplication", () => {
  beforeAll(async () => {
    db = await startTestDatabase();
  }, 60_000);

  afterAll(async () => {
    await stopTestDatabase(db);
  });

  it("section 8: two concurrent 80.00 BETs against a 100.00 balance settle to exactly 1 PROCESSED + 1 REJECTED", async () => {
    const walletId = await seedWallet("player-hot-1", "100.00");

    const request = (externalId: string): ProcessWagerInput => ({
      externalTransactionId: externalId,
      providerId: "provider-hot",
      idempotencyKey: `idem-${externalId}`,
      payloadHash: `hash-${externalId}`,
      kind: "BET",
      walletId,
      amount: Money.from({ amount: "80.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    });

    const [resultA, resultB] = await Promise.all([
      freshUseCase().execute(request("hot-bet-a")),
      freshUseCase().execute(request("hot-bet-b")),
    ]);

    const statuses = [resultA.status, resultB.status].sort();
    expect(statuses).toEqual(["PROCESSED", "REJECTED"]);

    const rejected = resultA.status === "REJECTED" ? resultA : resultB;
    if (rejected.status === "REJECTED") {
      expect(rejected.failureCode).toBe(FailureCode.INSUFFICIENT_FUNDS);
    }

    const verifyEm = db.orm.em.fork();
    const wallet = await new MikroOrmWalletRepository(verifyEm).findById(walletId);
    expect(wallet?.balance.toString()).toBe("20.00");
    const ledger = await new MikroOrmWalletRepository(verifyEm).listLedgerEntries(walletId, { limit: 100 });
    expect(ledger.filter((entry) => entry.direction === "DEBIT")).toHaveLength(1);
  }, 20_000);

  it("3+ concurrent instances: two race on the same wallet while a third processes an unrelated wallet", async () => {
    const hotWalletId = await seedWallet("player-hot-2", "100.00");
    const otherWalletId = await seedWallet("player-other-1", "50.00");

    const [hotA, hotB, other] = await Promise.all([
      freshUseCase().execute({
        externalTransactionId: "multi-hot-a",
        providerId: "provider-multi",
        idempotencyKey: "idem-multi-hot-a",
        payloadHash: "hash-multi-hot-a",
        kind: "BET",
        walletId: hotWalletId,
        amount: Money.from({ amount: "80.00", currency: "BRL" }),
        referenceExternalTransactionId: null,
      }),
      freshUseCase().execute({
        externalTransactionId: "multi-hot-b",
        providerId: "provider-multi",
        idempotencyKey: "idem-multi-hot-b",
        payloadHash: "hash-multi-hot-b",
        kind: "BET",
        walletId: hotWalletId,
        amount: Money.from({ amount: "80.00", currency: "BRL" }),
        referenceExternalTransactionId: null,
      }),
      freshUseCase().execute({
        externalTransactionId: "multi-other",
        providerId: "provider-multi",
        idempotencyKey: "idem-multi-other",
        payloadHash: "hash-multi-other",
        kind: "BET",
        walletId: otherWalletId,
        amount: Money.from({ amount: "10.00", currency: "BRL" }),
        referenceExternalTransactionId: null,
      }),
    ]);

    expect([hotA.status, hotB.status].sort()).toEqual(["PROCESSED", "REJECTED"]);
    expect(other.status).toBe("PROCESSED"); // unrelated wallet never blocked by the hot-wallet contention

    const verifyEm = db.orm.em.fork();
    const otherWallet = await new MikroOrmWalletRepository(verifyEm).findById(otherWalletId);
    expect(otherWallet?.balance.toString()).toBe("40.00");
  }, 20_000);

  it("the same bet sent 50 times in parallel produces exactly one debit and 49 idempotent replays", async () => {
    const walletId = await seedWallet("player-mass-1", "1000.00");

    const request: ProcessWagerInput = {
      externalTransactionId: "mass-bet-1",
      providerId: "provider-mass",
      idempotencyKey: "idem-mass-1",
      payloadHash: "hash-mass-1",
      kind: "BET",
      walletId,
      amount: Money.from({ amount: "25.00", currency: "BRL" }),
      referenceExternalTransactionId: null,
    };

    const results = await Promise.all(Array.from({ length: 50 }, () => freshUseCase().execute(request)));

    const processedFirstTime = results.filter((r) => !r.idempotentReplay);
    const replays = results.filter((r) => r.idempotentReplay);
    expect(processedFirstTime).toHaveLength(1);
    expect(replays).toHaveLength(49);
    expect(new Set(results.map((r) => r.transactionId)).size).toBe(1); // everyone agrees on the same transaction id

    const verifyEm = db.orm.em.fork();
    const wallet = await new MikroOrmWalletRepository(verifyEm).findById(walletId);
    expect(wallet?.balance.toString()).toBe("975.00"); // debited exactly once
  }, 30_000);
});
```

- [ ] **Step 2: Run the tests**

Run: `bun test test/concurrency-hot-wallet.spec.ts`
Expected: PASS (3 tests). If the mass-duplication test fails with an
unhandled unique-constraint error instead of 49 replays, the race-handling
`catch` block added to `ProcessWagerUseCase.execute` in Task 10 is missing
or not catching `UniqueConstraintViolationException` correctly — fix Task 10
before proceeding.

- [ ] **Step 3: Commit**

```bash
git add test/concurrency-hot-wallet.spec.ts
git commit -m "test: prove hot-wallet, multi-instance and mass-duplication concurrency scenarios"
```

---

## Task 20: Auth extension point + ARCHITECTURE.md and README.md

**Files:**
- Create: `src/shared-kernel/auth/identity-provider.port.ts`
- Create: `src/shared-kernel/auth/no-op-auth.guard.ts`
- Create: `ARCHITECTURE.md`
- Modify: `README.md` (replace the Nest-generated boilerplate)

**Interfaces:**
- Consumes: every decision made across Tasks 1-19 and the approved spec
  (`docs/superpowers/specs/2026-09-01-wagering-processor-architecture-design.md`).
- Produces:
  - `interface IdentityProviderPort { verifyToken(token: string): Promise<{ subject: string; roles: string[] } | null> }`
  - `class NoOpAuthGuard implements CanActivate` — always allows the
    request through; not registered anywhere (no `@UseGuards` applied to
    any controller). Exists purely as the explicit extension point the
    spec requires (§8) — wiring it in is future work, documented as such.
  - The two documentation deliverables the rubric explicitly grades
    (5 points: "README.md com setup e comandos, ARCHITECTURE.md com decisões,
    trade-offs e limitações").

Auth code has no automated test — a no-op guard has nothing to assert
beyond "it compiles and always returns true," which the build step already
covers. The rest of this task is prose; its "done" bar is the checklist in
Step 3.

- [ ] **Step 1: Write the auth extension point**

```typescript
// src/shared-kernel/auth/identity-provider.port.ts
/**
 * Not implemented — see ARCHITECTURE.md § Authentication. This is the
 * extension point a real integration (Keycloak/Zitadel via OIDC) would
 * implement; nothing in this codebase constructs one today.
 */
export interface IdentityProviderPort {
  verifyToken(token: string): Promise<{ subject: string; roles: string[] } | null>;
}
```

```typescript
// src/shared-kernel/auth/no-op-auth.guard.ts
import { CanActivate, ExecutionContext, Injectable } from "@nestjs/common";

/**
 * Not implemented — see ARCHITECTURE.md § Authentication. Always allows
 * the request through. Not registered on any controller in this codebase;
 * exists only as the explicit extension point the spec requires. Wiring
 * this in for real would mean replacing the body with a call to an
 * `IdentityProviderPort` implementation and applying `@UseGuards(...)`.
 */
@Injectable()
export class NoOpAuthGuard implements CanActivate {
  canActivate(_context: ExecutionContext): boolean {
    return true;
  }
}
```

- [ ] **Step 2: Write ARCHITECTURE.md**

```markdown
# Architecture

## Scope decision

This project does not target the full 100-point rubric. Priority order:
(1) the eliminatory requirements — Money never `number`/`float`, no negative
balance from a race, no duplicate debit/credit, persistent idempotency,
correctness with 3+ concurrent instances, an auditable ledger, integration
tests against real Postgres/SQS; (2) everything else (observability depth,
test breadth, messaging sophistication) kept simple and defensible over
built to maximize points. Load testing (an optional differential) was not
attempted.

## Stack

- **Bun** — runtime, package manager, native test runner (not vitest).
- **NestJS** — HTTP framework, dependency injection.
- **MikroORM** (`@mikro-orm/postgresql`) — chosen over TypeORM (the other
  acceptable option) for its explicit Unit of Work and
  `EntityManager.transactional()`, which map cleanly onto "one aggregate,
  one transaction." Prisma and Drizzle were excluded by the challenge brief.
- **PostgreSQL** — `NUMERIC(19,4)` for every monetary column: 2 digits of
  headroom over the 2-decimal boundary enforced at the HTTP edge, so
  intermediate ledger arithmetic never loses precision. `NUMERIC(19,2)`
  would also satisfy the brief; the extra scale was a defensive choice, not
  a requirement.
- **AWS SQS via LocalStack** — `wager-transactions.fifo` /
  `wager-transactions-dlq.fifo` (names mandated by the brief).
- **decimal.js** — backs the `Money` value object.
- **Testcontainers** — real Postgres and LocalStack in integration tests,
  never mocked.

## Domain model

`Money`, `Wallet`, `WagerTransaction`, `WalletLedgerEntry` are plain
TypeScript classes with zero imports from `@nestjs/*` or `@mikro-orm/*`
(explicit requirement, spec §6.1). Persistence is a separate MikroORM
entity per aggregate plus a `Mapper` translating both ways
(`infrastructure/persistence/mappers/`). The extra indirection buys
domain classes that are unit-testable without a database and unaffected by
ORM upgrades.

`Wallet.debit`/`credit` are immutable — they return a new `Wallet` plus the
`WalletLedgerEntry` that justifies the change, never mutate in place.
Insufficient funds and currency mismatches are **modeled as a result**
(`{ status: "REJECTED", failureCode }`), not thrown — that's expected
business flow, not an exceptional one.

`REFUND` may only reference a `BET`; `ROLLBACK` may reference `BET`, `WIN`,
or `REFUND`. A reversal whose reference hasn't arrived (or hasn't finished
processing) yet is stored as `PENDING_REFERENCE` — not rejected. It resolves
**synchronously**, inside the same SQL transaction that finally processes
the referenced transaction (no separate polling worker with backoff). This
is a deliberate simplification: if the referenced transaction never
arrives, the reversal stays `PENDING_REFERENCE` indefinitely. A production
system would add a TTL/expiry sweep; this one documents the gap instead of
building it, per the scope decision above.

## Concurrency

Pessimistic locking: `SELECT ... FOR UPDATE` on the wallet row, inside the
same transaction as the ledger insert, the wallet update, and the outbox
insert. Chosen over optimistic locking with retry or a conditional atomic
`UPDATE` because it resolves the mandatory hot-wallet scenario (two
concurrent 80.00 BETs against a 100.00 balance → exactly one `PROCESSED`,
one `REJECTED`) without a retry storm under contention. The trade-off:
writes to the same wallet serialize. That's accepted — the brief expects
it, and the lock is scoped to one row, so unrelated wallets never wait on
each other.

`ProcessWagerUseCase` also handles a second race: two concurrent requests
for the *same* `(providerId, externalTransactionId)` can both pass the
initial "does this transaction already exist?" check before either commits
— the loser's `INSERT` fails on the unique constraint at commit time. That
failure is caught and converted into a replay of the winner's result,
rather than surfacing as an error. Proven under real 50-way concurrency in
`test/concurrency-hot-wallet.spec.ts`.

**On testing "3+ instances":** the correctness property (row-level locks,
unique constraints) lives in Postgres, which cannot distinguish "3
concurrent connections from 3 forked EntityManagers in one Bun process"
from "3 separate containers" — both are genuinely concurrent transactions
over independent connections. The test suite exercises 3+ concurrent
`ProcessWagerUseCase` instances this way rather than orchestrating 3
separate `docker compose` app replicas. This is a scope simplification, not
a claim that the two are identical in every respect (network partitions
between real replicas aren't exercised).

## Idempotency

Two layers, both persistent (never in-memory):

- **HTTP** — `idempotency_keys` table, unique on `key`. Same key + same
  payload hash → cached response replay. Same key + different payload →
  409.
- **Messaging** — `inbox_messages` table, primary key
  `(consumer_name, message_id)`. Checked inside the same transaction as
  the debit/credit it guards.

Underneath both, `wager_transactions` has its own
`UNIQUE (provider_id, external_transaction_id)` — the actual source of
truth for "has this bet already been processed," independent of which
channel (HTTP or SQS) it arrived through, and independent of whether the
HTTP cache or inbox row was written. See the race-handling note above.

## Messaging: inbox and outbox

Outbox row is written in the same SQL transaction as the ledger — never
published before commit. A separate `OutboxPublisherWorker` (one per app
instance, all running the same code) polls with
`SELECT ... FOR UPDATE SKIP LOCKED`, so N publisher instances never claim
the same row. Backoff on publish failure is a fixed delay, not an
exponential curve — simpler to reason about, sufficient for this scope.

The SQS consumer and the HTTP controller both call the exact same
`ProcessWagerUseCase` (explicit requirement, spec §10) — no business logic
duplicated per entry point.

## Reconciliation

`POST /wallets/:walletId/reconciliation` is read-only. It recomputes the
balance directly in SQL from every ledger row
(`sum(CREDIT) - sum(DEBIT)`) and compares it to the materialized
`wallets.balance`. A mismatch is logged and returned as
`consistent: false` — never silently corrected.

## Schema-level invariants

Per the brief's explicit requirement (spec §11), the following are enforced
in Postgres, not only in application code:

- `wallets`: `CHECK (balance >= 0)`, `UNIQUE (player_id, currency)`.
- `wager_transactions`: `UNIQUE (idempotency_key)`,
  `UNIQUE (provider_id, external_transaction_id)`.
- `wallet_ledger_entries`: `UNIQUE (wallet_id, transaction_id)`, and a
  trigger (`prevent_ledger_mutation`) that raises on any `UPDATE`/`DELETE`
  — the ledger is append-only at the database level, not by convention.
- `inbox_messages`: `PRIMARY KEY (consumer_name, message_id)`.

## Authentication

**Not implemented**, per the brief's explicit statement that auth is worth
zero points and should not compete with financial correctness,
concurrency, and idempotency. If it were implemented: an external OIDC
provider (Keycloak or Zitadel), never a hand-rolled user table with
password hashing. The extension point is explicit in the code —
`src/shared-kernel/auth/identity-provider.port.ts` and
`no-op-auth.guard.ts` — but not wired into any controller: every endpoint
is currently unguarded. Wiring it in for real means implementing
`IdentityProviderPort` against an actual OIDC provider and applying
`@UseGuards(...)`; no controller logic would need to change beyond that,
since business rules never reference the caller's identity directly.
`/health/live` and `/health/ready` would stay open regardless.

## Observability

NestJS's default `Logger` — not the JSON-structured logging with
`correlationId`/`messageId`/`transactionId`/`walletId`/`providerId` fields
described in the original spec. Wiring a structured logger (e.g. pino) was
cut to keep this scope small; it's a mechanical follow-up, not a design
gap. No Prometheus/Grafana stack either. Transaction counts, DLQ depth, and
outbox lag are answered with a SQL query
against `wager_transactions`/`outbox_messages`, documented here rather than
wired into a dashboard:

```sql
-- outbox lag
select count(*), min(occurred_at)
from outbox_messages
where published_at is null;

-- transactions by status, last hour
select status, count(*)
from wager_transactions
where created_at > now() - interval '1 hour'
group by status;
```

## Known limitations

- `PENDING_REFERENCE` has no expiry — a reversal referencing a transaction
  that never arrives stays pending forever.
- The stored `amount` on a `REFUND`/`ROLLBACK` transaction reflects what the
  caller claimed, not necessarily what was actually reversed — the actual
  wallet effect always uses the *referenced* transaction's amount (the
  authoritative source), but no `REVERSAL_AMOUNT_MISMATCH` failure is
  raised if the two differ. A stricter version would compare and reject.
- No load testing was performed (optional differential, out of scope).
- No metrics/dashboard stack — SQL queries substitute for it.
- Logs are NestJS's default `Logger`, not JSON-structured with
  `correlationId`/`messageId`/etc. fields as originally sketched — a
  mechanical follow-up, not a design gap.
- The ≥3-instances concurrency tests use concurrent `EntityManager` forks
  in one process rather than separate containers (see the Concurrency
  section above).
```

- [ ] **Step 3: Write README.md**

```markdown
# Distributed Wagering Processor

Financial service that processes BET/WIN/LOSS/REFUND/ROLLBACK transactions
from HTTP and SQS, correct under concurrency, persistently idempotent, with
an auditable ledger. Built for the Jungle Gaming technical challenge.

See `ARCHITECTURE.md` for design decisions, trade-offs, and known
limitations. See `docs/superpowers/specs/2026-09-01-wagering-processor-architecture-design.md`
for the full design spec this was built from.

## Requirements

- [Bun](https://bun.sh) 1.x
- Docker + Docker Compose

## Setup

```bash
cp .env.example .env
docker compose up -d postgres localstack
bun install
bunx mikro-orm migration:up
bun run src/messaging/infrastructure/sqs/bootstrap-queues.ts
```

## Running

```bash
bun run start:dev        # local, against docker-compose postgres/localstack
# or
docker compose up -d     # full stack including the app container
```

The API listens on `http://localhost:3000`.

## Testing

```bash
bun test src              # unit tests (no containers required)
bun test test             # integration + concurrency tests (spins up
                           # Postgres and LocalStack via Testcontainers —
                           # requires Docker running, no manual setup)
```

`test/e2e-smoke.spec.ts` is the exception — it exercises the docker-compose
stack directly, so run `docker compose up -d`, apply migrations, and
bootstrap the queues (see Setup) before running it.

## API summary

| Method | Path | Purpose |
|---|---|---|
| POST | `/wallets` | Open a wallet (optional initial balance → `OPENING` transaction) |
| GET | `/wallets/:walletId` | Wallet state |
| GET | `/wallets/:walletId/ledger` | Paginated ledger (`?after=&limit=`) |
| POST | `/wallets/:walletId/reconciliation` | Recompute balance from the ledger, read-only |
| POST | `/wagering/transactions` | Submit BET/WIN/LOSS/REFUND/ROLLBACK (`Idempotency-Key` header required) |
| GET | `/wagering/transactions/:transactionId` | Look up a transaction by internal id |
| GET | `/providers/:providerId/wagering/transactions/:externalTransactionId` | Look up by provider + external id |
| GET | `/health/live` / `/health/ready` | Liveness / readiness (unauthenticated) |

## Not implemented

Authentication — a zero-point item per the challenge brief, deliberately
skipped to protect time for financial correctness, concurrency, and
idempotency. See `ARCHITECTURE.md` § Authentication.
```

- [ ] **Step 4: Self-review checklist**

- [ ] Every section of the approved spec has a corresponding paragraph here
      (Money, Wallet/Ledger, concurrency, idempotency, inbox/outbox,
      reconciliation, schema constraints, auth, observability).
- [ ] Every "Known limitation" is something actually true of the code as
      built in Tasks 1-19, not aspirational.
- [ ] `README.md` setup commands were actually run once, in order, on a
      clean checkout, and worked.

- [ ] **Step 5: Commit**

```bash
git add src/shared-kernel/auth/ ARCHITECTURE.md README.md
git commit -m "docs: add ARCHITECTURE.md, rewrite README.md, add no-op auth extension point"
```

---

## Summary of what this plan builds

20 tasks, in dependency order: `Money` → `Wallet`/`WalletLedgerEntry` →
`WagerTransaction` → MikroORM bootstrap → schema migration → Wallet
persistence (with the `FOR UPDATE` lock proof) → WagerTransaction/
IdempotencyKey persistence → Inbox/Outbox persistence (with the `SKIP
LOCKED` proof) → `CreateWalletUseCase` + `POST /wallets` →
`ProcessWagerUseCase` (the core, with the concurrent-duplicate race fix) →
HTTP idempotency + `POST /wagering/transactions` → read endpoints →
reconciliation → SQS bootstrap → SQS consumer → outbox publisher →
health → full `AppModule` wiring + e2e smoke → dedicated concurrency
proofs (hot wallet, 3+ instances, 50-way duplication) → documentation +
auth extension point.

Every eliminatory requirement from the spec has a task that proves it with
a real Postgres/LocalStack container, not a mock: Money never touches
`number` (Task 1), no negative balance under a race (Tasks 6, 19), no
duplicate debit (Tasks 10, 19), idempotency is persistent (Tasks 7, 11),
correctness holds with 3+ concurrent instances (Task 19), the ledger is
auditable and append-only at the schema level (Task 5).
