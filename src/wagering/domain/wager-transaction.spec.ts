import { describe, expect, it } from "bun:test";
import { Money } from "../../shared-kernel/money.js";
import { FailureCode } from "../../shared-kernel/failure-code.js";
import {
  isExternallySubmittableKind,
  validateReferenceKind,
  WagerTransaction,
} from "./wager-transaction.js";

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
