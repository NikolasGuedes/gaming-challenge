import { defineEntity, p } from "@mikro-orm/postgresql";

const WagerTransactionSchema = defineEntity({
  name: "WagerTransactionEntity",
  tableName: "wager_transactions",
  properties: {
    id: p.uuid().primary(),
    walletId: p.uuid(),
    externalTransactionId: p.string(),
    providerId: p.string(),
    idempotencyKey: p.string(),
    payloadHash: p.string(),
    kind: p.string(),
    amount: p.decimal().precision(19).scale(4),
    currency: p.string().length(3),
    referenceExternalTransactionId: p.text().strictNullable(),
    status: p.string(),
    failureCode: p.text().strictNullable(),
    resultBalanceAmount: p.decimal().precision(19).scale(4).strictNullable(),
    resultBalanceCurrency: p.string().length(3).strictNullable(),
    createdAt: p.datetime().onCreate(() => new Date()),
    updatedAt: p
      .datetime()
      .onCreate(() => new Date())
      .onUpdate(() => new Date()),
  },
});

export class WagerTransactionEntity extends WagerTransactionSchema.class {}
WagerTransactionSchema.setClass(WagerTransactionEntity);
