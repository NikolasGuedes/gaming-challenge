import { Migration } from "@mikro-orm/migrations";

export class Migration20260902000000 extends Migration {
  async up(): Promise<void> {
    this.addSql(`alter table "wager_transactions" add column "player_id" text null;`);
    this.addSql(`alter table "wager_transactions" add column "round_id" text null;`);
    this.addSql(`alter table "wager_transactions" add column "game_id" text null;`);
    this.addSql(`alter table "wager_transactions" add column "reference_transaction_id" uuid null references "wager_transactions" ("id");`);
    this.addSql(`alter table "wager_transactions" add column "processed_at" timestamptz null;`);
    this.addSql(`alter table "wager_transactions" add column "reference_attempts" int not null default 0;`);
    this.addSql(`alter table "wager_transactions" add column "next_reference_attempt_at" timestamptz null;`);
    this.addSql(`alter table "wager_transactions" add column "reference_expires_at" timestamptz null;`);

    this.addSql(`
      update "wager_transactions" wt
      set "player_id" = w."player_id",
          "round_id" = case when wt."kind" = 'OPENING' then 'internal-opening' else 'legacy-round' end,
          "game_id" = case when wt."kind" = 'OPENING' then 'internal-opening' else 'legacy-game' end,
          "processed_at" = case when wt."status" in ('PROCESSED', 'REJECTED', 'FAILED') then wt."updated_at" else null end
      from "wallets" w
      where w."id" = wt."wallet_id";
    `);
    this.addSql(`alter table "wager_transactions" alter column "player_id" set not null;`);
    this.addSql(`alter table "wager_transactions" alter column "round_id" set not null;`);
    this.addSql(`alter table "wager_transactions" alter column "game_id" set not null;`);
    this.addSql(`
      update "wager_transactions" reversal
      set "reference_transaction_id" = referenced."id"
      from "wager_transactions" referenced
      where reversal."provider_id" = referenced."provider_id"
        and reversal."reference_external_transaction_id" = referenced."external_transaction_id";
    `);
    this.addSql(`
      create unique index "wager_transactions_processed_reversal_unique"
      on "wager_transactions" ("reference_transaction_id")
      where "status" = 'PROCESSED' and "reference_transaction_id" is not null;
    `);
    this.addSql(`
      create index "wager_transactions_pending_reference_lookup"
      on "wager_transactions" ("status", "next_reference_attempt_at")
      where "status" = 'PENDING_REFERENCE';
    `);
  }

  async down(): Promise<void> {
    this.addSql(`drop index if exists "wager_transactions_processed_reversal_unique";`);
    this.addSql(`drop index if exists "wager_transactions_pending_reference_lookup";`);
    this.addSql(`alter table "wager_transactions" drop column "reference_expires_at";`);
    this.addSql(`alter table "wager_transactions" drop column "next_reference_attempt_at";`);
    this.addSql(`alter table "wager_transactions" drop column "reference_attempts";`);
    this.addSql(`alter table "wager_transactions" drop column "processed_at";`);
    this.addSql(`alter table "wager_transactions" drop column "reference_transaction_id";`);
    this.addSql(`alter table "wager_transactions" drop column "game_id";`);
    this.addSql(`alter table "wager_transactions" drop column "round_id";`);
    this.addSql(`alter table "wager_transactions" drop column "player_id";`);
  }
}
