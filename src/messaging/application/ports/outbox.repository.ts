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
  append(message: {
    id: string;
    aggregateId: string;
    eventType: string;
    payload: unknown;
    occurredAt: Date;
  }): Promise<void>;
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
