# Distributed Wagering Processor

Financial service that processes BET/WIN/LOSS/REFUND/ROLLBACK transactions
from HTTP and SQS, correct under concurrency, persistently idempotent, with
an auditable ledger. Built for the Jungle Gaming technical challenge.

See `ARCHITECTURE.md` for design decisions, trade-offs, and known
limitations. See `docs/superpowers/specs/2026-09-01-wagering-processor-architecture-design.md`
for the full design spec this was built from.

## Requirements

- [Bun](https://bun.sh) 1.x
- Docker + Docker Compose

## Setup

```bash
cp .env.example .env
docker compose up -d postgres localstack
bun install
bunx mikro-orm migration:up
bun run src/messaging/infrastructure/sqs/bootstrap-queues.ts
```

If `bunx mikro-orm ...` fails in your environment with a `node:fs`/`globSync`
error (a known issue on systems whose default `node` predates Node 22's
`fs.globSync`, since the MikroORM CLI shebang resolves to system `node`, not
`bun`), run the CLI through Bun directly instead — equivalent, and what was
actually used to verify this README:

```bash
bun run node_modules/@mikro-orm/cli/cli.js migration:up
```

## Running

```bash
bun run start:dev        # local, against docker-compose postgres/localstack
# or
bun run build && bun run start:prod   # compiled build, closer to production
# or
docker compose up -d     # full stack including the app container
```

The API listens on `http://localhost:3000`. Verified directly against this
worktree: `bun run build` + `bun run start:prod` boots, `/health/live` and
`/health/ready` respond, and `POST /wallets` → `POST /wagering/transactions`
→ `GET /wallets/:id/ledger` → `POST /wallets/:id/reconciliation` all round-trip
correctly end to end against real Postgres and LocalStack.

`bun run start:dev` shells out to the system `node` binary (via the Nest
CLI's watcher), so it can hit the same `node:fs`/`globSync` issue noted
above depending on your environment; `bun run start:prod` (after `bun run
build`) does not, since it runs the compiled output directly with `bun`.

## Testing

```bash
bun test src              # unit tests (no containers required)
bun test test              # integration + concurrency tests (spins up
                           # Postgres and LocalStack via Testcontainers —
                           # requires Docker running, no manual setup)
```

Both are green as of this task: `bun test test/ src` → **60 pass, 0 fail**
across 18 files, including every mandatory concurrency scenario from the
challenge brief (hot-wallet contention, 3+ concurrent instances, 50-way
duplicate-request replay).

`test/e2e-smoke.spec.ts` is the exception — it exercises the docker-compose
stack directly, so run `docker compose up -d`, apply migrations, and
bootstrap the queues (see Setup) before running it.

## API summary

| Method | Path | Purpose |
|---|---|---|
| POST | `/wallets` | Open a wallet (optional initial balance → `OPENING` transaction) |
| GET | `/wallets/:walletId` | Wallet state |
| GET | `/wallets/:walletId/ledger` | Paginated ledger (`?after=&limit=`) |
| POST | `/wallets/:walletId/reconciliation` | Recompute balance from the ledger, read-only |
| POST | `/wagering/transactions` | Submit BET/WIN/LOSS/REFUND/ROLLBACK (`Idempotency-Key` header required) |
| GET | `/wagering/transactions/:transactionId` | Look up a transaction by internal id |
| GET | `/providers/:providerId/wagering/transactions/:externalTransactionId` | Look up by provider + external id |
| GET | `/health/live` / `/health/ready` | Liveness / readiness (unauthenticated) |

## Not implemented

Authentication — a zero-point item per the challenge brief, deliberately
skipped to protect time for financial correctness, concurrency, and
idempotency. See `ARCHITECTURE.md` § Authentication.

Reliable delivery of `WagerProcessed` outbound notifications to an external
consumer — the outbox publisher and the SQS wager-submission consumer
currently share the same queue (the two names the brief mandates), so a
published notification loops back and is rejected by the consumer rather
than reaching a real subscriber. Harmless (no partial writes; the failure
rolls back cleanly) but noisy, and out of scope to fix under this
challenge's grading criteria. See `ARCHITECTURE.md` § Known limitations.
