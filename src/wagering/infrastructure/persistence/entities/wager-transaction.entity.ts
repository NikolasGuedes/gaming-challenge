import { defineEntity, p } from "@mikro-orm/postgresql";

const WagerTransactionSchema = defineEntity({
  name: "WagerTransactionEntity",
  tableName: "wager_transactions",
  properties: {
    id: p.uuid().primary(),
    walletId: p.uuid(),
    playerId: p.string(),
    roundId: p.string(),
    gameId: p.string(),
    externalTransactionId: p.string(),
    providerId: p.string(),
    idempotencyKey: p.string(),
    payloadHash: p.string(),
    kind: p.string(),
    amount: p.decimal().precision(19).scale(4),
    currency: p.string().length(3),
    referenceExternalTransactionId: p.text().strictNullable(),
    referenceTransactionId: p.uuid().strictNullable(),
    status: p.string(),
    failureCode: p.text().strictNullable(),
    resultBalanceAmount: p.decimal().precision(19).scale(4).strictNullable(),
    resultBalanceCurrency: p.string().length(3).strictNullable(),
    processedAt: p.datetime().strictNullable(),
    referenceAttempts: p.integer().default(0),
    nextReferenceAttemptAt: p.datetime().strictNullable(),
    referenceExpiresAt: p.datetime().strictNullable(),
    createdAt: p.datetime().onCreate(() => new Date()),
    updatedAt: p
      .datetime()
      .onCreate(() => new Date())
      .onUpdate(() => new Date()),
  },
});

export class WagerTransactionEntity extends WagerTransactionSchema.class {}
WagerTransactionSchema.setClass(WagerTransactionEntity);
