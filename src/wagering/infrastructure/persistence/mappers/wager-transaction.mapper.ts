import { Money } from "../../../../shared-kernel/money.js";
import { WagerTransaction, WagerKind, WagerStatus } from "../../../domain/wager-transaction.js";
import { FailureCode } from "../../../../shared-kernel/failure-code.js";
import { WagerTransactionEntity } from "../entities/wager-transaction.entity.js";

export class WagerTransactionMapper {
  static toDomain(entity: WagerTransactionEntity): WagerTransaction {
    return WagerTransaction.rehydrate({
      id: entity.id,
      walletId: entity.walletId,
      playerId: entity.playerId,
      roundId: entity.roundId,
      gameId: entity.gameId,
      externalTransactionId: entity.externalTransactionId,
      providerId: entity.providerId,
      idempotencyKey: entity.idempotencyKey,
      payloadHash: entity.payloadHash,
      kind: entity.kind as WagerKind,
      amount: Money.rehydrate({ amount: entity.amount, currency: entity.currency }),
      referenceExternalTransactionId: entity.referenceExternalTransactionId,
      referenceTransactionId: entity.referenceTransactionId,
      status: entity.status as WagerStatus,
      failureCode: entity.failureCode as FailureCode | null,
      resultBalance:
        entity.resultBalanceAmount && entity.resultBalanceCurrency
          ? Money.rehydrate({ amount: entity.resultBalanceAmount, currency: entity.resultBalanceCurrency })
          : null,
      processedAt: entity.processedAt,
      referenceAttempts: entity.referenceAttempts,
      nextReferenceAttemptAt: entity.nextReferenceAttemptAt,
      referenceExpiresAt: entity.referenceExpiresAt,
      createdAt: entity.createdAt,
      updatedAt: entity.updatedAt,
    });
  }

  static toEntity(tx: WagerTransaction, existing?: WagerTransactionEntity): WagerTransactionEntity {
    const entity = existing ?? new WagerTransactionEntity();
    entity.id = tx.id;
    entity.walletId = tx.walletId;
    entity.playerId = tx.playerId;
    entity.roundId = tx.roundId;
    entity.gameId = tx.gameId;
    entity.externalTransactionId = tx.externalTransactionId;
    entity.providerId = tx.providerId;
    entity.idempotencyKey = tx.idempotencyKey;
    entity.payloadHash = tx.payloadHash;
    entity.kind = tx.kind;
    entity.amount = tx.amount.toString();
    entity.currency = tx.amount.currency;
    entity.referenceExternalTransactionId = tx.referenceExternalTransactionId;
    entity.referenceTransactionId = tx.referenceTransactionId;
    entity.status = tx.status;
    entity.failureCode = tx.failureCode;
    entity.resultBalanceAmount = tx.resultBalance ? tx.resultBalance.toString() : null;
    entity.resultBalanceCurrency = tx.resultBalance ? tx.resultBalance.currency : null;
    entity.processedAt = tx.processedAt;
    entity.referenceAttempts = tx.referenceAttempts;
    entity.nextReferenceAttemptAt = tx.nextReferenceAttemptAt;
    entity.referenceExpiresAt = tx.referenceExpiresAt;
    return entity;
  }
}
