import { Migration } from '@mikro-orm/migrations';

export class Migration20260901000000 extends Migration {
  async up(): Promise<void> {
    this.addSql(`
      create table "wallets" (
        "id" uuid not null primary key,
        "player_id" text not null,
        "currency" char(3) not null,
        "balance" numeric(19,4) not null default 0,
        "version" int not null default 1,
        "created_at" timestamptz not null default now(),
        "updated_at" timestamptz not null default now(),
        constraint "wallets_balance_non_negative" check ("balance" >= 0),
        constraint "wallets_player_currency_unique" unique ("player_id", "currency")
      );
    `);

    this.addSql(`
      create table "wager_transactions" (
        "id" uuid not null primary key,
        "wallet_id" uuid not null references "wallets" ("id"),
        "external_transaction_id" text not null,
        "provider_id" text not null,
        "idempotency_key" text not null,
        "payload_hash" text not null,
        "kind" text not null check ("kind" in ('BET','WIN','LOSS','REFUND','ROLLBACK','OPENING')),
        "amount" numeric(19,4) not null,
        "currency" char(3) not null,
        "reference_external_transaction_id" text null,
        "status" text not null check ("status" in ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        "failure_code" text null,
        "result_balance_amount" numeric(19,4) null,
        "result_balance_currency" char(3) null,
        "created_at" timestamptz not null default now(),
        "updated_at" timestamptz not null default now(),
        constraint "wager_transactions_idempotency_key_unique" unique ("idempotency_key"),
        constraint "wager_transactions_provider_external_unique" unique ("provider_id", "external_transaction_id")
      );
    `);

    this.addSql(`
      create table "wallet_ledger_entries" (
        "id" uuid not null primary key,
        "wallet_id" uuid not null references "wallets" ("id"),
        "transaction_id" uuid not null references "wager_transactions" ("id"),
        "direction" text not null check ("direction" in ('DEBIT','CREDIT')),
        "amount" numeric(19,4) not null,
        "currency" char(3) not null,
        "balance_before" numeric(19,4) not null,
        "balance_after" numeric(19,4) not null,
        "created_at" timestamptz not null default now(),
        constraint "wallet_ledger_entries_wallet_transaction_unique" unique ("wallet_id", "transaction_id")
      );
    `);

    // Ledger is append-only: no UPDATE or DELETE, enforced at the schema level.
    this.addSql(`
      create function "prevent_ledger_mutation"() returns trigger as $$
      begin
        raise exception 'wallet_ledger_entries is append-only: % is not allowed', TG_OP;
      end;
      $$ language plpgsql;
    `);
    this.addSql(`
      create trigger "wallet_ledger_entries_no_mutation"
      before update or delete on "wallet_ledger_entries"
      for each row execute function "prevent_ledger_mutation"();
    `);

    this.addSql(`
      create table "idempotency_keys" (
        "key" text not null primary key,
        "payload_hash" text not null,
        "response" jsonb not null,
        "created_at" timestamptz not null default now()
      );
    `);

    this.addSql(`
      create table "inbox_messages" (
        "consumer_name" text not null,
        "message_id" text not null,
        "processed_at" timestamptz not null default now(),
        primary key ("consumer_name", "message_id")
      );
    `);

    this.addSql(`
      create table "outbox_messages" (
        "id" uuid not null primary key,
        "aggregate_id" uuid not null,
        "event_type" text not null,
        "payload" jsonb not null,
        "occurred_at" timestamptz not null,
        "attempts" int not null default 0,
        "next_attempt_at" timestamptz not null default now(),
        "published_at" timestamptz null
      );
    `);
    this.addSql(`
      create index "outbox_messages_publish_lookup"
      on "outbox_messages" ("published_at", "next_attempt_at");
    `);
  }

  async down(): Promise<void> {
    this.addSql(`drop table if exists "outbox_messages" cascade;`);
    this.addSql(`drop table if exists "inbox_messages" cascade;`);
    this.addSql(`drop table if exists "idempotency_keys" cascade;`);
    this.addSql(`drop trigger if exists "wallet_ledger_entries_no_mutation" on "wallet_ledger_entries";`);
    this.addSql(`drop function if exists "prevent_ledger_mutation";`);
    this.addSql(`drop table if exists "wallet_ledger_entries" cascade;`);
    this.addSql(`drop table if exists "wager_transactions" cascade;`);
    this.addSql(`drop table if exists "wallets" cascade;`);
  }
}
