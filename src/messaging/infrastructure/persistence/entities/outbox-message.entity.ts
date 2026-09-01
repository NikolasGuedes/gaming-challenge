import { defineEntity, p } from "@mikro-orm/postgresql";

const OutboxMessageSchema = defineEntity({
  name: "OutboxMessageEntity",
  tableName: "outbox_messages",
  properties: {
    id: p.uuid().primary(),
    aggregateId: p.uuid(),
    eventType: p.text(),
    payload: p.json(),
    occurredAt: p.datetime(),
    attempts: p.integer().default(0),
    nextAttemptAt: p.datetime().onCreate(() => new Date()),
    publishedAt: p.datetime().strictNullable(),
  },
});

export class OutboxMessageEntity extends OutboxMessageSchema.class {}
OutboxMessageSchema.setClass(OutboxMessageEntity);
