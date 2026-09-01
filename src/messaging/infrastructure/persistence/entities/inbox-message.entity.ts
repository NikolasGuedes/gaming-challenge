import { defineEntity, p } from "@mikro-orm/postgresql";

const InboxMessageSchema = defineEntity({
  name: "InboxMessageEntity",
  tableName: "inbox_messages",
  properties: {
    consumerName: p.text().primary(),
    messageId: p.text().primary(),
    processedAt: p.datetime().onCreate(() => new Date()),
  },
});

export class InboxMessageEntity extends InboxMessageSchema.class {}
InboxMessageSchema.setClass(InboxMessageEntity);
