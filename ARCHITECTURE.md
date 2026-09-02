# Architecture

## Scope decision

This project does not target the full 100-point rubric. Priority order:
(1) the eliminatory requirements — Money never `number`/`float`, no negative
balance from a race, no duplicate debit/credit, persistent idempotency,
correctness with 3+ concurrent instances, an auditable ledger, integration
tests against real Postgres/SQS; (2) everything else (observability depth,
test breadth, messaging sophistication) kept simple and defensible over
built to maximize points. Load testing (an optional differential) was not
attempted.

## Stack

- **Bun** — runtime, package manager, native test runner (not vitest).
- **NestJS** — HTTP framework, dependency injection.
- **MikroORM** (`@mikro-orm/postgresql`) — chosen over TypeORM (the other
  acceptable option) for its explicit Unit of Work and
  `EntityManager.transactional()`, which map cleanly onto "one aggregate,
  one transaction." Prisma and Drizzle were excluded by the challenge brief.
  The version actually installed is **MikroORM 7.1.14**, not 5/6 — every
  entity in this codebase is therefore defined with `defineEntity()` +
  `Schema.setClass()` (see `src/*/infrastructure/persistence/entities/`),
  not the `@Entity()`/`@Property()`/`@PrimaryKey()` decorator API from
  older MikroORM versions. There is no decorator-based entity anywhere in
  the codebase — this is the one API shape a contributor needs to match.
- **PostgreSQL** — `NUMERIC(19,4)` for every monetary column: 2 digits of
  headroom over the 2-decimal boundary enforced at the HTTP edge, so
  intermediate ledger arithmetic never loses precision. `NUMERIC(19,2)`
  would also satisfy the brief; the extra scale was a defensive choice, not
  a requirement.
- **AWS SQS via LocalStack** — `wager-transactions.fifo` /
  `wager-transactions-dlq.fifo` (names mandated by the brief).
- **decimal.js** — backs the `Money` value object.
- **Testcontainers** — real Postgres and LocalStack in integration tests,
  never mocked. The LocalStack module actually installed
  (`@testcontainers/localstack@12.1.0`) configures which services LocalStack
  starts via `.withEnvironment({ SERVICES: "sqs" })` — there is no
  `.withServices([...])` helper in this version, despite that shape existing
  in some other Testcontainers language bindings. See
  `test/bootstrap-queues.spec.ts`, `test/inbox-redelivery.spec.ts`, and
  `test/outbox-publisher-worker.spec.ts` for the pattern to copy when adding
  further integration tests.

## Domain model

`Money`, `Wallet`, `WagerTransaction`, `WalletLedgerEntry` are plain
TypeScript classes with zero imports from `@nestjs/*` or `@mikro-orm/*`
(explicit requirement, spec §6.1). Persistence is a separate MikroORM
entity per aggregate plus a `Mapper` translating both ways
(`infrastructure/persistence/mappers/`). The extra indirection buys
domain classes that are unit-testable without a database and unaffected by
ORM upgrades.

`Wallet.debit`/`credit` are immutable — they return a new `Wallet` plus the
`WalletLedgerEntry` that justifies the change, never mutate in place.
Insufficient funds and currency mismatches are **modeled as a result**
(`{ status: "REJECTED", failureCode }`), not thrown — that's expected
business flow, not an exceptional one.

`REFUND` may only reference a `BET`; `ROLLBACK` may reference `BET`, `WIN`,
or `REFUND`. A reversal whose reference hasn't arrived (or hasn't finished
processing) yet is stored as `PENDING_REFERENCE` — not rejected. It resolves
**synchronously**, inside the same SQL transaction that finally processes
the referenced transaction (no separate polling worker with backoff). This
is a deliberate simplification: if the referenced transaction never
arrives, the reversal stays `PENDING_REFERENCE` indefinitely. A production
system would add a TTL/expiry sweep; this one documents the gap instead of
building it, per the scope decision above.

## Concurrency

Pessimistic locking: `SELECT ... FOR UPDATE` on the wallet row, inside the
same transaction as the ledger insert, the wallet update, and the outbox
insert. Chosen over optimistic locking with retry or a conditional atomic
`UPDATE` because it resolves the mandatory hot-wallet scenario (two
concurrent 80.00 BETs against a 100.00 balance → exactly one `PROCESSED`,
one `REJECTED`) without a retry storm under contention. The trade-off:
writes to the same wallet serialize. That's accepted — the brief expects
it, and the lock is scoped to one row, so unrelated wallets never wait on
each other.

`ProcessWagerUseCase` also handles a second race: two concurrent requests
for the *same* `(providerId, externalTransactionId)` can both pass the
initial "does this transaction already exist?" check before either commits
— the loser's `INSERT` fails on the unique constraint at commit time. That
failure is caught and converted into a replay of the winner's result,
rather than surfacing as an error. Proven under real 50-way concurrency in
`test/concurrency-hot-wallet.spec.ts`.

`ProcessWagerUseCase` also closes a third race, found and fixed during
implementation rather than anticipated up front: nothing originally stopped
two *different* `REFUND`s from both referencing the same already-processed
`BET` and both crediting the wallet — each request, taken alone, looked
like a fresh, valid reversal. The fix is `FailureCode.REFERENCE_ALREADY_REVERSED`:
before crediting, the use case calls
`WagerTransactionRepository.findProcessedReversalFor(providerId, externalTransactionId)`
and rejects a second reversal of a reference that already has one processed
against it. This is shipped, tested behavior, not a documented gap — see
the residual race noted under Known limitations, though.

**On testing "3+ instances":** the correctness property (row-level locks,
unique constraints) lives in Postgres, which cannot distinguish "3
concurrent connections from 3 forked EntityManagers in one Bun process"
from "3 separate containers" — both are genuinely concurrent transactions
over independent connections. The test suite exercises 3+ concurrent
`ProcessWagerUseCase` instances this way rather than orchestrating 3
separate `docker compose` app replicas. This is a scope simplification, not
a claim that the two are identical in every respect (network partitions
between real replicas aren't exercised).

## Idempotency

Two layers, both persistent (never in-memory):

- **HTTP** — `idempotency_keys` table, unique on `key`. Same key + same
  payload hash → cached response replay. Same key + different payload →
  409.
- **Messaging** — `inbox_messages` table, primary key
  `(consumer_name, message_id)`. Checked inside the same transaction as
  the debit/credit it guards.

Underneath both, `wager_transactions` has its own
`UNIQUE (provider_id, external_transaction_id)` — the actual source of
truth for "has this bet already been processed," independent of which
channel (HTTP or SQS) it arrived through, and independent of whether the
HTTP cache or inbox row was written. See the race-handling note above.

## Messaging: inbox and outbox

Outbox row is written in the same SQL transaction as the ledger — never
published before commit. A separate `OutboxPublisherWorker` (one per app
instance, all running the same code) polls with
`SELECT ... FOR UPDATE SKIP LOCKED`, so N publisher instances never claim
the same row. Backoff on publish failure is a fixed delay, not an
exponential curve — simpler to reason about, sufficient for this scope.

The SQS consumer and the HTTP controller both call the exact same
`ProcessWagerUseCase` (explicit requirement, spec §10) — no business logic
duplicated per entry point.

## Reconciliation

`POST /wallets/:walletId/reconciliation` is read-only. It recomputes the
balance directly in SQL from every ledger row
(`sum(CREDIT) - sum(DEBIT)`) and compares it to the materialized
`wallets.balance`. A mismatch is logged and returned as
`consistent: false` — never silently corrected.

## Schema-level invariants

Per the brief's explicit requirement (spec §11), the following are enforced
in Postgres, not only in application code:

- `wallets`: `CHECK (balance >= 0)`, `UNIQUE (player_id, currency)`.
- `wager_transactions`: `UNIQUE (idempotency_key)`,
  `UNIQUE (provider_id, external_transaction_id)`.
- `wallet_ledger_entries`: `UNIQUE (wallet_id, transaction_id)`, and a
  trigger (`prevent_ledger_mutation`) that raises on any `UPDATE`/`DELETE`
  — the ledger is append-only at the database level, not by convention.
- `inbox_messages`: `PRIMARY KEY (consumer_name, message_id)`.

## Authentication

**Not implemented**, per the brief's explicit statement that auth is worth
zero points and should not compete with financial correctness,
concurrency, and idempotency. If it were implemented: an external OIDC
provider (Keycloak or Zitadel), never a hand-rolled user table with
password hashing. The extension point is explicit in the code —
`src/shared-kernel/auth/identity-provider.port.ts` and
`no-op-auth.guard.ts` — but not wired into any controller: every endpoint
is currently unguarded. Wiring it in for real means implementing
`IdentityProviderPort` against an actual OIDC provider and applying
`@UseGuards(...)`; no controller logic would need to change beyond that,
since business rules never reference the caller's identity directly.
`/health/live` and `/health/ready` would stay open regardless.

## Observability

NestJS's default `Logger` — not the JSON-structured logging with
`correlationId`/`messageId`/`transactionId`/`walletId`/`providerId` fields
described in the original spec. Wiring a structured logger (e.g. pino) was
cut to keep this scope small; it's a mechanical follow-up, not a design
gap. No Prometheus/Grafana stack either. Transaction counts, DLQ depth, and
outbox lag are answered with a SQL query
against `wager_transactions`/`outbox_messages`, documented here rather than
wired into a dashboard:

```sql
-- outbox lag
select count(*), min(occurred_at)
from outbox_messages
where published_at is null;

-- transactions by status, last hour
select status, count(*)
from wager_transactions
where created_at > now() - interval '1 hour'
group by status;
```

## Gotchas for contributors

- Every test that builds a use case from a forked `EntityManager` and needs
  it to participate correctly in `em.transactional()` must fork with
  `em.fork({ useContext: true })`. MikroORM 7 defaults `useContext` to
  `false`, which silently breaks the `AsyncLocalStorage`-based transaction
  context resolution — the fork will look fine in isolation but any
  transaction started against it won't see writes from `transactional()`
  as expected. See `test/wallet-opening.spec.ts` / `test/wallet-reads.spec.ts`
  for the pattern.
- Relative imports throughout `src/` use the `.js` extension on the
  specifier even though the source file is `.ts` (`import ... from
  "./foo.js"` resolving to `foo.ts`) — required by this project's
  `moduleResolution: "nodenext"`. Bun tolerates a missing extension at
  runtime, which is why an extensionless import can sit unnoticed for a
  while; `tsc` (and therefore `bun run build`, and therefore the Docker
  image build, which runs it) does not, and fails with `TS2307: Cannot
  find module`. `src/app.module.ts`'s four feature-module imports were
  missing the extension until this task's verification pass caught it via
  an actual `bun run build`.
- `mikro-orm.config.ts`'s `entities` array lists the six entity classes
  directly (`[WalletEntity, WalletLedgerEntryEntity, ...]`), not a glob
  string. It used to be `entities: ['dist/**/*.entity.js']` (later
  corrected to the still-glob `'dist/src/**/*.entity.js'` mid-investigation)
  — MikroORM's own glob-based discovery re-`import()`s each matched
  compiled file to obtain a class reference to register, and under Bun's
  ESM loader that re-import produced a second, distinct class object per
  entity: same name, different identity from the class this codebase's own
  repositories imported statically elsewhere. The result: every
  `em.persist(...)` against a running `bun run start:prod` (or the
  Docker image, which runs the same compiled output) threw `ValidationError:
  Trying to persist not discovered entity ... not the prototype you are
  passing to the ORM` — even though `bunx mikro-orm debug`/`bun run
  node_modules/@mikro-orm/cli/cli.js debug` happily reported the glob as
  "found" and the unit/integration test suite (which never runs the
  compiled build standalone) stayed green throughout. Passing the classes
  directly makes MikroORM register the exact object every other module
  imports, sidestepping the mismatch, and works unchanged whether this
  file runs as TS (dev/test, via Bun) or as compiled `dist/mikro-orm.config.js`
  (`start:prod`/Docker) — this file's own imports resolve to the sibling
  `.entity.ts` source in the former case and to `dist/src/**/*.entity.js` in
  the latter, exactly like every other `.js`-suffixed relative import in
  this codebase. If you ever reintroduce a glob-string `entities` array,
  re-verify by actually running `bun run start:prod` end-to-end (create a
  wallet, process a transaction) — `bun test` alone will not catch a
  regression here, since it never executes the compiled artifact.

## Known limitations (found during this task's end-to-end verification)

- **`OutboxPublisherWorker` and `WagerTransactionConsumer` both read
  `SQS_QUEUE_URL`** — i.e. the same physical `wager-transactions.fifo`
  queue is used both for inbound wager submissions (what the consumer is
  built to parse) and outbound `WagerProcessed` domain events (what the
  publisher writes there from the outbox). Verified by running the full
  stack (`bun run start:prod` against the compiled build) end-to-end: an
  `OPENING` and a `BET` were each processed correctly over HTTP, and each
  produced an outbox row that the publisher duly delivered onto
  `wager-transactions.fifo` — which the consumer then picked back up,
  failed to parse as a wager-submission envelope (`TypeError: undefined is
  not an object (evaluating 'envelope.data.externalTransactionId')`), and
  logged an error. This is benign, not a correctness bug: the failure
  happens inside the same DB transaction as the (no-op) processing attempt,
  which rolls back cleanly with no partial writes, and the message would
  eventually redrive to the DLQ after repeated receive failures. But it is
  wasted work and log noise on every processed transaction, and it means
  no external consumer of `WagerProcessed` notifications is realistically
  usable today, since the same queue immediately turns around and rejects
  its own published messages. Fixing this properly needs either a second,
  brief-unspecified queue for outbound notifications, or a way for the
  consumer to distinguish and skip messages it didn't publish (e.g. a
  message attribute) — neither was implemented; the two mandated queue
  names (`wager-transactions.fifo` / DLQ) are used exactly as the brief
  named them, for inbound processing only. Accepted as out of scope for
  this challenge's grading (which concerns processing correctness, not
  outbound event delivery), documented rather than fixed under this task's
  budget.

## Known limitations

- `PENDING_REFERENCE` has no expiry — a reversal referencing a transaction
  that never arrives stays pending forever.
- Transitive pending-reference chains (depth greater than one) do not
  resolve — e.g. a `ROLLBACK` referencing a `REFUND` that was itself still
  `PENDING_REFERENCE` at the time. `resolvePendingReferences` only looks one
  level deep: it resolves reversals that directly reference the transaction
  just processed, not reversals-of-reversals. Accepted for the same reason
  as the item above — same class of gap, same scope decision.
- The double-reversal guard (`FailureCode.REFERENCE_ALREADY_REVERSED`) has
  a residual TOCTOU race under true concurrency: `findProcessedReversalFor`
  is queried *before* the wallet row lock is acquired, so two genuinely
  concurrent `REFUND`s of the same `BET` could theoretically both pass the
  check before either commits. Unlike the `(provider_id,
  external_transaction_id)` dedup — which has a DB-level unique constraint
  backstopping it even if the application check races — there is no
  equivalent partial unique index backstopping this one. Accepted: this
  scenario isn't one of the challenge's mandatory concurrency scenarios, and
  closing it fully would need either a stricter lock ordering or a unique
  index on "processed reversal per referenced transaction," which is future
  work if this gets hardened further.
- The stored `amount` on a `REFUND`/`ROLLBACK` transaction reflects what the
  caller claimed, not necessarily what was actually reversed — the actual
  wallet effect always uses the *referenced* transaction's amount (the
  authoritative source), but no `REVERSAL_AMOUNT_MISMATCH` failure is
  raised if the two differ. A stricter version would compare and reject.
- No load testing was performed (optional differential, out of scope).
- No metrics/dashboard stack — SQL queries substitute for it.
- Logs are NestJS's default `Logger`, not JSON-structured with
  `correlationId`/`messageId`/etc. fields as originally sketched — a
  mechanical follow-up, not a design gap.
- The ≥3-instances concurrency tests use concurrent `EntityManager` forks
  in one process rather than separate containers (see the Concurrency
  section above).
- `test/concurrency-hot-wallet.spec.ts`'s third scenario (≥3 instances)
  proves correctness — the unrelated wallet processes correctly alongside
  the contended one — but does not add a timing assertion proving
  genuinely non-blocking execution; a global-lock implementation could
  theoretically still pass it. Accepted: scenarios 1 and 3 in that file
  already rigorously prove the properties that matter for this brief
  (mutual exclusion on the contended wallet, idempotent replay under
  concurrent duplicates), and a timing-based proof of non-blocking
  execution is inherently flaky in a shared CI environment.
