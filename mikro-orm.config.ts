import 'dotenv/config';
import { defineConfig } from '@mikro-orm/postgresql';
import { WalletEntity } from './src/wallet/infrastructure/persistence/entities/wallet.entity.js';
import { WalletLedgerEntryEntity } from './src/wallet/infrastructure/persistence/entities/wallet-ledger-entry.entity.js';
import { WagerTransactionEntity } from './src/wagering/infrastructure/persistence/entities/wager-transaction.entity.js';
import { IdempotencyKeyEntity } from './src/wagering/infrastructure/persistence/entities/idempotency-key.entity.js';
import { InboxMessageEntity } from './src/messaging/infrastructure/persistence/entities/inbox-message.entity.js';
import { OutboxMessageEntity } from './src/messaging/infrastructure/persistence/entities/outbox-message.entity.js';

// Entities are listed as direct class references, not glob strings.
// MikroORM's glob-based discovery re-`import()`s each matched file itself
// (dist/src/**/*.entity.js) to obtain a class reference to register — under
// Bun's ESM loader that re-import produced a *second*, distinct class
// object per entity (same name, different identity from the one this
// codebase's own repositories imported statically), which made every
// `em.persist(...)` throw `ValidationError: Trying to persist not
// discovered entity ... not the prototype you are passing to the ORM` at
// runtime under the compiled build (`bun run start:prod` / Docker), even
// though `bunx mikro-orm debug` reported the glob as "found". Importing the
// classes directly here means MikroORM registers the exact same object
// every other module in the app imports, sidestepping that mismatch
// entirely — and works identically whether this file runs as TS (via Bun,
// in dev/test) or as the compiled dist/mikro-orm.config.js (in production),
// since these imports resolve to the sibling .entity.ts source in the
// former case and to dist/src/**/*.entity.js in the latter.
const entities = [
  WalletEntity,
  WalletLedgerEntryEntity,
  WagerTransactionEntity,
  IdempotencyKeyEntity,
  InboxMessageEntity,
  OutboxMessageEntity,
];

export default defineConfig({
  clientUrl: process.env.DATABASE_URL,
  entities,
  migrations: {
    // rootDir is "." (see tsconfig.build.json), so compiled output nests
    // under dist/src/**; src/migrations/*.ts therefore compiles to
    // dist/src/migrations, not dist/migrations.
    path: 'dist/src/migrations',
    pathTs: 'src/migrations',
  },
  discovery: { warnWhenNoEntities: false },
  debug: process.env.NODE_ENV !== 'production',
});
