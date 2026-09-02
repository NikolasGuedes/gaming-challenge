import { WagerTransaction } from "../../domain/wager-transaction.js";

export interface WagerTransactionRepository {
  findById(id: string): Promise<WagerTransaction | null>;
  findByIdForUpdate(id: string): Promise<WagerTransaction | null>;
  findByProviderAndExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | null>;
  findByIdempotencyKey(idempotencyKey: string): Promise<WagerTransaction | null>;
  save(tx: WagerTransaction): Promise<void>;
  /** PENDING_REFERENCE rows whose reference points at (providerId, externalTransactionId). */
  findPendingReferencesFor(providerId: string, externalTransactionId: string): Promise<WagerTransaction[]>;
  /** An already-PROCESSED REFUND/ROLLBACK whose reference points at (providerId, externalTransactionId), if any. */
  findProcessedReversalFor(providerId: string, externalTransactionId: string): Promise<WagerTransaction | null>;
  findDuePendingReferenceIds(now: Date, limit: number): Promise<string[]>;
}

export const WAGER_TRANSACTION_REPOSITORY = Symbol("WAGER_TRANSACTION_REPOSITORY");
