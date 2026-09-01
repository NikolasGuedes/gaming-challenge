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
