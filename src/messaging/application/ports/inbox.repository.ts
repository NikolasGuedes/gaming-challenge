export interface InboxRepository {
  /** Must be called inside the same transaction as the business write it guards. */
  exists(consumerName: string, messageId: string): Promise<boolean>;
  markProcessed(consumerName: string, messageId: string): Promise<void>;
}

export const INBOX_REPOSITORY = Symbol("INBOX_REPOSITORY");
