import { ConflictException, Inject, Injectable } from "@nestjs/common";
import { EntityManager } from "@mikro-orm/postgresql";
import { computePayloadHash } from "../../domain/payload-hash.js";
import {
  IDEMPOTENCY_KEY_REPOSITORY,
  type IdempotencyKeyRepository,
} from "../../application/ports/idempotency-key.repository.js";

@Injectable()
export class IdempotencyService {
  constructor(
    @Inject(IDEMPOTENCY_KEY_REPOSITORY) private readonly idempotencyKeyRepository: IdempotencyKeyRepository,
    // MikroOrmIdempotencyKeyRepository#save() only persists (repositories in this codebase
    // never flush themselves — the enclosing use case controls the commit boundary via
    // em.transactional()). This service sits outside of ProcessWagerUseCase's own transaction,
    // so it owns flushing the idempotency record itself.
    private readonly em: EntityManager,
  ) {}

  /**
   * Fast-path HTTP replay cache. Even if this were skipped entirely, replaying
   * the same (providerId, externalTransactionId) still resolves correctly
   * through ProcessWagerUseCase's idempotency-key and domain-level uniqueness checks — this
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
        throw new ConflictException(`Idempotency-Key ${key} was already used with a different payload`);
      }
      return { response: existing.response as T, idempotentReplay: true };
    }

    const response = await execute();
    await this.idempotencyKeyRepository.save({ key, payloadHash, response });
    await this.em.flush();
    return { response, idempotentReplay: false };
  }
}
