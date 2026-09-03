import { PostgreSqlContainer, StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { Wait } from "testcontainers";
import { MikroORM } from "@mikro-orm/postgresql";
import config from "../../mikro-orm.config.js";

export interface TestDatabase {
  container: StartedPostgreSqlContainer;
  orm: MikroORM;
}

export async function startTestDatabase(): Promise<TestDatabase> {
  const container = await new PostgreSqlContainer("postgres:16-alpine")
    .withDatabase("wagering_test")
    .withUsername("wagering")
    .withPassword("wagering")
    // The image healthcheck occasionally reports an intermediate state to the
    // Docker-in-Docker CI runner. PostgreSQL emits this line twice (bootstrap
    // server and final server), so waiting for both is deterministic.
    .withWaitStrategy(Wait.forLogMessage("database system is ready to accept connections", 2))
    .start();

  const orm = await MikroORM.init({
    ...config,
    clientUrl: container.getConnectionUri(),
  });
  await orm.migrator.up();
  return { container, orm };
}

export async function stopTestDatabase(db: TestDatabase | undefined): Promise<void> {
  if (!db) return;
  await db.orm.close(true);
  await db.container.stop();
}
