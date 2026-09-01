import { defineEntity, p } from "@mikro-orm/postgresql";

const IdempotencyKeySchema = defineEntity({
  name: "IdempotencyKeyEntity",
  tableName: "idempotency_keys",
  properties: {
    key: p.text().primary(),
    payloadHash: p.string(),
    response: p.json<unknown>(),
    createdAt: p.datetime().onCreate(() => new Date()),
  },
});

export class IdempotencyKeyEntity extends IdempotencyKeySchema.class {}
IdempotencyKeySchema.setClass(IdempotencyKeyEntity);
