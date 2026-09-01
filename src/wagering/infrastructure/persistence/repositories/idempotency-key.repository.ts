import { Injectable } from "@nestjs/common";
import { EntityManager } from "@mikro-orm/postgresql";
import {
  IdempotencyKeyRecord,
  IdempotencyKeyRepository,
} from "../../../application/ports/idempotency-key.repository.js";
import { IdempotencyKeyEntity } from "../entities/idempotency-key.entity.js";

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
