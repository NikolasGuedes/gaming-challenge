import { defineEntity, p } from "@mikro-orm/postgresql";

const WalletSchema = defineEntity({
  name: "WalletEntity",
  tableName: "wallets",
  properties: {
    id: p.uuid().primary(),
    playerId: p.string(),
    currency: p.string().length(3),
    balance: p.decimal().precision(19).scale(4),
    version: p.integer(),
    createdAt: p.datetime().onCreate(() => new Date()),
    updatedAt: p
      .datetime()
      .onCreate(() => new Date())
      .onUpdate(() => new Date()),
  },
});

export class WalletEntity extends WalletSchema.class {}
WalletSchema.setClass(WalletEntity);
