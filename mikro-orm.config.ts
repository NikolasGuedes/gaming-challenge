import 'dotenv/config';
import { defineConfig } from '@mikro-orm/postgresql';

export default defineConfig({
  clientUrl: process.env.DATABASE_URL,
  entities: ['dist/**/*.entity.js'],
  entitiesTs: ['src/**/*.entity.ts'],
  migrations: {
    path: 'dist/migrations',
    pathTs: 'src/migrations',
  },
  // No entities exist yet (added starting in Task 5); without this, MikroORM
  // throws MetadataError.noEntityDiscovered() on boot instead of starting cleanly.
  discovery: { warnWhenNoEntities: false },
  debug: process.env.NODE_ENV !== 'production',
});
