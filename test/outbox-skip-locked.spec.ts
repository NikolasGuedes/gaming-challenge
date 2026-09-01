import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { MikroOrmOutboxRepository } from "../src/messaging/infrastructure/persistence/repositories/outbox.repository.js";
import { OutboxMessageRecord } from "../src/messaging/application/ports/outbox.repository.js";
import { startTestDatabase, stopTestDatabase, TestDatabase } from "./support/testcontainers-env.js";

describe("MikroOrmOutboxRepository — SKIP LOCKED", () => {
  let db: TestDatabase;

  beforeAll(async () => {
    db = await startTestDatabase();
  }, 60_000);

  afterAll(async () => {
    await stopTestDatabase(db);
  });

  it("two concurrent publishers never claim the same row", async () => {
    const setupEm = db.orm.em.fork();
    const setupRepo = new MikroOrmOutboxRepository(setupEm, db.orm);
    await setupRepo.append({
      id: "66666666-6666-6666-6666-666666666666",
      aggregateId: "11111111-1111-1111-1111-111111111111",
      eventType: "WagerProcessed",
      payload: { hello: "1" },
      occurredAt: new Date(),
    });
    await setupRepo.append({
      id: "77777777-7777-7777-7777-777777777777",
      aggregateId: "22222222-2222-2222-2222-222222222222",
      eventType: "WagerProcessed",
      payload: { hello: "2" },
      occurredAt: new Date(),
    });
    await setupEm.flush();

    const publishedByInstance1: string[] = [];
    const publishedByInstance2: string[] = [];

    const instance1 = new MikroOrmOutboxRepository(db.orm.em.fork(), db.orm);
    const instance2 = new MikroOrmOutboxRepository(db.orm.em.fork(), db.orm);

    await Promise.all([
      instance1.processDueBatch(1, async (message: OutboxMessageRecord) => {
        publishedByInstance1.push(message.id);
        return true;
      }),
      instance2.processDueBatch(1, async (message: OutboxMessageRecord) => {
        publishedByInstance2.push(message.id);
        return true;
      }),
    ]);

    const allPublished = [...publishedByInstance1, ...publishedByInstance2];
    expect(allPublished).toHaveLength(2);
    expect(new Set(allPublished).size).toBe(2); // no row claimed twice
  }, 20_000);
});
