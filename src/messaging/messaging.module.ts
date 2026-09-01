import { Module } from "@nestjs/common";
import { OUTBOX_REPOSITORY } from "./application/ports/outbox.repository.js";
import { MikroOrmOutboxRepository } from "./infrastructure/persistence/repositories/outbox.repository.js";

// Minimal stub (Task 12): provides just enough for WageringModule to compile and
// for ProcessWagerUseCase (which depends on OUTBOX_REPOSITORY) to run. Task 16
// overwrites this file with the full version — SQS_CLIENT, INBOX_REPOSITORY, the
// SQS consumer, and the outbox publisher worker — as an additive superset of
// this stub, not a conflicting rewrite.
@Module({
  providers: [{ provide: OUTBOX_REPOSITORY, useClass: MikroOrmOutboxRepository }],
  exports: [OUTBOX_REPOSITORY],
})
export class MessagingModule {}
