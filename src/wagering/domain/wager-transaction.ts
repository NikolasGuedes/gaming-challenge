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
    readonly playerId: string,
    readonly roundId: string,
    readonly gameId: string,
    readonly externalTransactionId: string,
    readonly providerId: string,
    readonly idempotencyKey: string,
    readonly payloadHash: string,
    readonly kind: WagerKind,
    readonly amount: Money,
    readonly referenceExternalTransactionId: string | null,
    readonly referenceTransactionId: string | null,
    readonly status: WagerStatus,
    readonly failureCode: FailureCode | null,
    readonly resultBalance: Money | null,
    readonly processedAt: Date | null,
    readonly referenceAttempts: number,
    readonly nextReferenceAttemptAt: Date | null,
    readonly referenceExpiresAt: Date | null,
    readonly createdAt: Date,
    readonly updatedAt: Date,
  ) {}

  static create(input: {
    id: string;
    walletId: string;
    playerId: string;
    roundId: string;
    gameId: string;
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
      input.playerId,
      input.roundId,
      input.gameId,
      input.externalTransactionId,
      input.providerId,
      input.idempotencyKey,
      input.payloadHash,
      input.kind,
      input.amount,
      input.referenceExternalTransactionId,
      null,
      "PENDING",
      null,
      null,
      null,
      0,
      null,
      null,
      now,
      now,
    );
  }

  static rehydrate(input: {
    id: string;
    walletId: string;
    playerId: string;
    roundId: string;
    gameId: string;
    externalTransactionId: string;
    providerId: string;
    idempotencyKey: string;
    payloadHash: string;
    kind: WagerKind;
    amount: Money;
    referenceExternalTransactionId: string | null;
    referenceTransactionId: string | null;
    status: WagerStatus;
    failureCode: FailureCode | null;
    resultBalance: Money | null;
    processedAt: Date | null;
    referenceAttempts: number;
    nextReferenceAttemptAt: Date | null;
    referenceExpiresAt: Date | null;
    createdAt: Date;
    updatedAt: Date;
  }): WagerTransaction {
    return new WagerTransaction(
      input.id,
      input.walletId,
      input.playerId,
      input.roundId,
      input.gameId,
      input.externalTransactionId,
      input.providerId,
      input.idempotencyKey,
      input.payloadHash,
      input.kind,
      input.amount,
      input.referenceExternalTransactionId,
      input.referenceTransactionId,
      input.status,
      input.failureCode,
      input.resultBalance,
      input.processedAt,
      input.referenceAttempts,
      input.nextReferenceAttemptAt,
      input.referenceExpiresAt,
      input.createdAt,
      input.updatedAt,
    );
  }

  private with(patch: Partial<{
    status: WagerStatus;
    failureCode: FailureCode | null;
    resultBalance: Money | null;
    referenceTransactionId: string | null;
    processedAt: Date | null;
    referenceAttempts: number;
    nextReferenceAttemptAt: Date | null;
    referenceExpiresAt: Date | null;
  }>): WagerTransaction {
    return new WagerTransaction(
      this.id,
      this.walletId,
      this.playerId,
      this.roundId,
      this.gameId,
      this.externalTransactionId,
      this.providerId,
      this.idempotencyKey,
      this.payloadHash,
      this.kind,
      this.amount,
      this.referenceExternalTransactionId,
      patch.referenceTransactionId !== undefined ? patch.referenceTransactionId : this.referenceTransactionId,
      patch.status ?? this.status,
      patch.failureCode !== undefined ? patch.failureCode : this.failureCode,
      patch.resultBalance !== undefined ? patch.resultBalance : this.resultBalance,
      patch.processedAt !== undefined ? patch.processedAt : this.processedAt,
      patch.referenceAttempts ?? this.referenceAttempts,
      patch.nextReferenceAttemptAt !== undefined ? patch.nextReferenceAttemptAt : this.nextReferenceAttemptAt,
      patch.referenceExpiresAt !== undefined ? patch.referenceExpiresAt : this.referenceExpiresAt,
      this.createdAt,
      new Date(),
    );
  }

  markProcessed(resultBalance: Money, referenceTransactionId: string | null = null): WagerTransaction {
    return this.with({
      status: "PROCESSED",
      failureCode: null,
      resultBalance,
      referenceTransactionId,
      processedAt: new Date(),
    });
  }

  markRejected(failureCode: FailureCode): WagerTransaction {
    return this.with({ status: "REJECTED", failureCode, resultBalance: null, processedAt: new Date() });
  }

  markPendingReference(): WagerTransaction {
    const now = new Date();
    return this.with({
      status: "PENDING_REFERENCE",
      referenceAttempts: 0,
      nextReferenceAttemptAt: new Date(now.getTime() + 1_000),
      referenceExpiresAt: new Date(now.getTime() + 5 * 60_000),
    });
  }

  scheduleReferenceRetry(now: Date): WagerTransaction {
    const attempts = this.referenceAttempts + 1;
    const delayMs = Math.min(2 ** attempts, 30) * 1_000;
    return this.with({ referenceAttempts: attempts, nextReferenceAttemptAt: new Date(now.getTime() + delayMs) });
  }

  hasExhaustedReferenceRetries(now: Date, maxAttempts = 5): boolean {
    return this.referenceAttempts + 1 >= maxAttempts || (this.referenceExpiresAt !== null && this.referenceExpiresAt <= now);
  }
}
