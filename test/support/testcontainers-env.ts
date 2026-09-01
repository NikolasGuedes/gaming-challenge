import { PostgreSqlContainer, StartedPostgreSqlContainer } from "@testcontainers/postgresql";
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
    .start();

  const orm = await MikroORM.init({
    ...config,
    clientUrl: container.getConnectionUri(),
  });
  await orm.migrator.up();
  return { container, orm };
}

export async function stopTestDatabase(db: TestDatabase): Promise<void> {
  await db.orm.close(true);
  await db.container.stop();
}
