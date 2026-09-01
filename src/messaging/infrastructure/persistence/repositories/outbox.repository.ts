import { Injectable } from "@nestjs/common";
import { EntityManager, MikroORM } from "@mikro-orm/postgresql";
import { OutboxMessageRecord, OutboxRepository } from "../../../application/ports/outbox.repository.js";
import { OutboxMessageEntity } from "../entities/outbox-message.entity.js";

@Injectable()
export class MikroOrmOutboxRepository implements OutboxRepository {
  constructor(
    private readonly em: EntityManager,
    private readonly orm: MikroORM,
  ) {}

  async append(message: {
    id: string;
    aggregateId: string;
    eventType: string;
    payload: unknown;
    occurredAt: Date;
  }): Promise<void> {
    const entity = new OutboxMessageEntity();
    entity.id = message.id;
    entity.aggregateId = message.aggregateId;
    entity.eventType = message.eventType;
    entity.payload = message.payload;
    entity.occurredAt = message.occurredAt;
    entity.attempts = 0;
    this.em.persist(entity);
  }

  async processDueBatch(
    limit: number,
    publish: (message: OutboxMessageRecord) => Promise<boolean>,
  ): Promise<number> {
    const fork = this.orm.em.fork();
    let processed = 0;
    await fork.transactional(async (tx) => {
      const rows = (await tx.getConnection().execute(
        `select id, aggregate_id, event_type, payload, occurred_at, attempts
         from outbox_messages
         where published_at is null and next_attempt_at <= now()
         order by occurred_at
         limit ?
         for update skip locked`,
        [limit],
      )) as Array<Record<string, unknown>>;

      for (const row of rows) {
        const record: OutboxMessageRecord = {
          id: row.id as string,
          aggregateId: row.aggregate_id as string,
          eventType: row.event_type as string,
          payload: row.payload,
          occurredAt: row.occurred_at as Date,
          attempts: row.attempts as number,
        };
        const ok = await publish(record);
        if (ok) {
          await tx.getConnection().execute(
            `update outbox_messages set published_at = now() where id = ?`,
            [record.id],
          );
        } else {
          await tx.getConnection().execute(
            `update outbox_messages set attempts = attempts + 1, next_attempt_at = now() + interval '5 seconds' where id = ?`,
            [record.id],
          );
        }
        processed += 1;
      }
    });
    return processed;
  }
}
