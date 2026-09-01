import 'dotenv/config';
import { defineConfig } from '@mikro-orm/postgresql';

export default defineConfig({
  clientUrl: process.env.DATABASE_URL,
  entities: ['dist/**/*.entity.js'],
  entitiesTs: ['src/**/*.entity.ts'],
  migrations: {
    // NOTE: rootDir is "." (see tsconfig.build.json), so compiled output
    // nests under dist/src/**. Once Task 5+ adds src/migrations/*.ts, the
    // compiled files will land at dist/src/migrations, not dist/migrations,
    // and this path must become "dist/src/migrations" to match.
    path: 'dist/migrations',
    pathTs: 'src/migrations',
  },
  // No entities exist yet (added starting in Task 5); without this, MikroORM
  // throws MetadataError.noEntityDiscovered() on boot instead of starting cleanly.
  discovery: { warnWhenNoEntities: false },
  debug: process.env.NODE_ENV !== 'production',
});
