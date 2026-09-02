import { Money } from "../../shared-kernel/money.js";
import { FailureCode } from "../../shared-kernel/failure-code.js";

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

/**
 * A REFUND/ROLLBACK moves money on the wallet the referenced transaction belongs to — never on
 * a wallet named by the caller. A submission whose walletId disagrees with the referenced
 * transaction's wallet would otherwise credit an unrelated wallet: money created system-wide,
 * invisible to per-wallet reconciliation (each wallet still matches its own ledger).
 */
export function validateReversalWallet(
  submittedWalletId: string,
  referencedWalletId: string,
): FailureCode | null {
  return submittedWalletId === referencedWalletId ? null : FailureCode.WALLET_MISMATCH;
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
