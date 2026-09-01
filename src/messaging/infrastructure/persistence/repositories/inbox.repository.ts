import { Injectable } from "@nestjs/common";
import { EntityManager } from "@mikro-orm/postgresql";
import { InboxRepository } from "../../../application/ports/inbox.repository.js";
import { InboxMessageEntity } from "../entities/inbox-message.entity.js";

@Injectable()
export class MikroOrmInboxRepository implements InboxRepository {
  constructor(private readonly em: EntityManager) {}

  async exists(consumerName: string, messageId: string): Promise<boolean> {
    const entity = await this.em.findOne(InboxMessageEntity, { consumerName, messageId });
    return entity !== null;
  }

  async markProcessed(consumerName: string, messageId: string): Promise<void> {
    const entity = new InboxMessageEntity();
    entity.consumerName = consumerName;
    entity.messageId = messageId;
    this.em.persist(entity);
  }
}
