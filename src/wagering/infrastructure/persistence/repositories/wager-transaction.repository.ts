import { Injectable } from "@nestjs/common";
import { EntityManager, LockMode } from "@mikro-orm/postgresql";
import { WagerTransaction } from "../../../domain/wager-transaction.js";
import { WagerTransactionRepository } from "../../../application/ports/wager-transaction.repository.js";
import { WagerTransactionEntity } from "../entities/wager-transaction.entity.js";
import { WagerTransactionMapper } from "../mappers/wager-transaction.mapper.js";

@Injectable()
export class MikroOrmWagerTransactionRepository implements WagerTransactionRepository {
  constructor(private readonly em: EntityManager) {}

  async findById(id: string): Promise<WagerTransaction | null> {
    const entity = await this.em.findOne(WagerTransactionEntity, { id });
    return entity ? WagerTransactionMapper.toDomain(entity) : null;
  }

  async findByIdForUpdate(id: string): Promise<WagerTransaction | null> {
    const entity = await this.em.findOne(WagerTransactionEntity, { id }, { lockMode: LockMode.PESSIMISTIC_WRITE });
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

  async findProcessedReversalFor(providerId: string, externalTransactionId: string): Promise<WagerTransaction | null> {
    const entity = await this.em.findOne(WagerTransactionEntity, {
      providerId,
      referenceExternalTransactionId: externalTransactionId,
      kind: { $in: ["REFUND", "ROLLBACK"] },
      status: "PROCESSED",
    });
    return entity ? WagerTransactionMapper.toDomain(entity) : null;
  }

  async findDuePendingReferenceIds(now: Date, limit: number): Promise<string[]> {
    const rows = await this.em.find(
      WagerTransactionEntity,
      { status: "PENDING_REFERENCE", nextReferenceAttemptAt: { $lte: now } },
      { fields: ["id"], orderBy: { nextReferenceAttemptAt: "asc" }, limit },
    );
    return rows.map((row) => row.id);
  }
}
