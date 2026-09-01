import 'dotenv/config';
import { defineConfig } from '@mikro-orm/postgresql';

export default defineConfig({
  clientUrl: process.env.DATABASE_URL,
  entities: ['dist/**/*.entity.js'],
  entitiesTs: ['src/**/*.entity.ts'],
  migrations: {
    // rootDir is "." (see tsconfig.build.json), so compiled output nests
    // under dist/src/**; src/migrations/*.ts therefore compiles to
    // dist/src/migrations, not dist/migrations.
    path: 'dist/src/migrations',
    pathTs: 'src/migrations',
  },
  // No entities exist yet (added starting in Task 5); without this, MikroORM
  // throws MetadataError.noEntityDiscovered() on boot instead of starting cleanly.
  discovery: { warnWhenNoEntities: false },
  debug: process.env.NODE_ENV !== 'production',
});
