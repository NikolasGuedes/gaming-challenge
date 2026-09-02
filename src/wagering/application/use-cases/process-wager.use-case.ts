import { Inject, Injectable, NotFoundException } from "@nestjs/common";
import { EntityManager, UniqueConstraintViolationException } from "@mikro-orm/postgresql";
import { Money } from "../../../shared-kernel/money.js";
import { FailureCode } from "../../../shared-kernel/failure-code.js";
import { IntegrationEvent } from "../../../shared-kernel/integration-event.js";
import { Wallet, type WalletApplyResult } from "../../../wallet/domain/wallet.js";
import { WalletLedgerEntry } from "../../../wallet/domain/wallet-ledger-entry.js";
import {
  WALLET_REPOSITORY,
  type WalletRepository,
} from "../../../wallet/application/ports/wallet.repository.js";
import { OUTBOX_REPOSITORY, type OutboxRepository } from "../../../messaging/application/ports/outbox.repository.js";
import {
  type WagerKind,
  type WagerStatus,
  WagerTransaction,
  isExternallySubmittableKind,
  validateReferenceKind,
  validateReversalWallet,
} from "../../domain/wager-transaction.js";
import {
  WagerEventData,
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from "../../domain/wager-events.js";
import {
  WAGER_TRANSACTION_REPOSITORY,
  type WagerTransactionRepository,
} from "../ports/wager-transaction.repository.js";

export interface ProcessWagerInput {
  externalTransactionId: string;
  providerId: string;
  /** Required by HTTP/SQS adapters; optional only for direct legacy use-case callers. */
  playerId?: string;
  roundId?: string;
  gameId?: string;
  idempotencyKey: string;
  payloadHash: string;
  kind: WagerKind;
  walletId: string;
  amount: Money;
  referenceExternalTransactionId: string | null;
}

export type ProcessWagerResult = (
  | { status: "PROCESSED"; transactionId: string; balance: Money }
  | { status: "REJECTED"; transactionId: string; failureCode: FailureCode }
  | { status: "PENDING_REFERENCE"; transactionId: string }
) & { idempotentReplay: boolean };

function originalDirectionOf(kind: WagerKind): "DEBIT" | "CREDIT" {
  return kind === "BET" ? "DEBIT" : "CREDIT";
}

/**
 * Every rule a REFUND/ROLLBACK must satisfy against the transaction it reverses, in one place.
 *
 * There are two paths that can apply a reversal — the direct submission path (the reference was
 * already PROCESSED when the reversal arrived) and the pending-resolution path (the reversal
 * arrived first, waited as PENDING_REFERENCE, and is resolved when the reference lands). Both
 * call this, so they cannot drift apart: whatever one rejects, the other rejects identically.
 *
 * `referencedWalletId` is always the wallet that is actually about to be credited (the locked
 * wallet), never a value the caller supplied.
 */
function reversalRejection(input: {
  kind: WagerKind;
  submittedWalletId: string;
  referencedKind: WagerKind;
  referencedWalletId: string;
  submittedPlayerId: string;
  referencedPlayerId: string;
  submittedRoundId: string;
  referencedRoundId: string;
  submittedAmount: Money;
  referencedAmount: Money;
  alreadyReversed: boolean;
}): FailureCode | null {
  return (
    validateReferenceKind(input.kind, input.referencedKind) ??
    validateReversalWallet(input.submittedWalletId, input.referencedWalletId) ??
    (input.submittedPlayerId !== input.referencedPlayerId ? FailureCode.PLAYER_MISMATCH : null) ??
    (input.submittedRoundId !== input.referencedRoundId ? FailureCode.ROUND_MISMATCH : null) ??
    (input.submittedAmount.currency !== input.referencedAmount.currency ? FailureCode.CURRENCY_MISMATCH : null) ??
    (!input.submittedAmount.equals(input.referencedAmount) ? FailureCode.REVERSAL_AMOUNT_MISMATCH : null) ??
    (input.alreadyReversed ? FailureCode.REFERENCE_ALREADY_REVERSED : null)
  );
}

@Injectable()
export class ProcessWagerUseCase {
  constructor(
    private readonly em: EntityManager,
    @Inject(WALLET_REPOSITORY) private readonly walletRepository: WalletRepository,
    @Inject(WAGER_TRANSACTION_REPOSITORY)
    private readonly wagerTransactionRepository: WagerTransactionRepository,
    @Inject(OUTBOX_REPOSITORY) private readonly outboxRepository: OutboxRepository,
  ) {}

  async execute(input: ProcessWagerInput): Promise<ProcessWagerResult> {
    try {
      // em.transactional() runs the callback with this same EntityManager switched into an
      // active DB transaction — every repository built from `this.em` (injected via Nest DI,
      // shared across this use case's constructor) automatically participates in it.
      return await this.em.transactional(() => this.executeWithinTransaction(input));
    } catch (err) {
      if (err instanceof UniqueConstraintViolationException) {
        // Another concurrent request for the same (providerId, externalTransactionId) won the
        // race and committed first — this is the expected outcome of "same bet sent N times in
        // parallel": exactly one debit, everyone else replays. Re-read the now-committed row.
        const winner = await this.wagerTransactionRepository.findByProviderAndExternalId(
          input.providerId,
          input.externalTransactionId,
        );
        if (winner) {
          return this.toResult(winner, true);
        }
      }
      throw err;
    }
  }

  /** Reprocesses one persisted out-of-order reversal. Safe for concurrent workers. */
  async retryPendingReference(transactionId: string, now = new Date()): Promise<WagerStatus | null> {
    return this.em.transactional(async () => {
      const snapshot = await this.wagerTransactionRepository.findById(transactionId);
      if (!snapshot || snapshot.status !== "PENDING_REFERENCE") {
        return snapshot?.status ?? null;
      }
      // Keep lock ordering identical to normal processing: wallet first, transaction second.
      // This avoids a deadlock with an original transaction resolving this pending row inline.
      let wallet = await this.walletRepository.findByIdForUpdate(snapshot.walletId);
      if (!wallet) {
        return null; // protected by the FK; defensive only
      }
      const pending = await this.wagerTransactionRepository.findByIdForUpdate(transactionId);
      if (!pending || pending.status !== "PENDING_REFERENCE") {
        return pending?.status ?? null;
      }
      if (pending.nextReferenceAttemptAt && pending.nextReferenceAttemptAt > now) {
        return pending.status;
      }
      const referenced = pending.referenceExternalTransactionId
        ? await this.wagerTransactionRepository.findByProviderAndExternalId(
            pending.providerId,
            pending.referenceExternalTransactionId,
          )
        : null;

      if (!referenced || referenced.status !== "PROCESSED") {
        if (pending.hasExhaustedReferenceRetries(now)) {
          const rejected = pending.markRejected(FailureCode.REFERENCE_NOT_FOUND);
          await this.wagerTransactionRepository.save(rejected);
          await this.publishRejected(rejected);
          return rejected.status;
        }
        await this.wagerTransactionRepository.save(pending.scheduleReferenceRetry(now));
        return pending.status;
      }

      const alreadyReversed = pending.referenceExternalTransactionId
        ? (await this.wagerTransactionRepository.findProcessedReversalFor(
            pending.providerId,
            pending.referenceExternalTransactionId,
          )) !== null
        : false;
      const rejection = reversalRejection({
        kind: pending.kind,
        submittedWalletId: pending.walletId,
        referencedKind: referenced.kind,
        referencedWalletId: referenced.walletId,
        submittedPlayerId: pending.playerId,
        referencedPlayerId: referenced.playerId,
        submittedRoundId: pending.roundId,
        referencedRoundId: referenced.roundId,
        submittedAmount: pending.amount,
        referencedAmount: referenced.amount,
        alreadyReversed,
      });
      if (rejection) {
        const rejected = pending.markRejected(rejection);
        await this.wagerTransactionRepository.save(rejected);
        await this.publishRejected(rejected);
        return rejected.status;
      }

      const applyResult = this.applyToWallet(wallet, pending.kind, referenced.kind, referenced.amount, pending.id);
      if (applyResult.status === "REJECTED") {
        const failureCode = applyResult.failureCode === FailureCode.INSUFFICIENT_FUNDS
          ? FailureCode.REVERSAL_WOULD_MAKE_BALANCE_NEGATIVE
          : applyResult.failureCode;
        const rejected = pending.markRejected(failureCode);
        await this.wagerTransactionRepository.save(rejected);
        await this.publishRejected(rejected);
        return rejected.status;
      }

      wallet = applyResult.wallet;
      const processed = pending.markProcessed(wallet.balance, referenced.id);
      await this.wagerTransactionRepository.save(processed);
      await this.em.flush();
      await this.walletRepository.appendLedgerEntry(applyResult.entry);
      await this.walletRepository.save(wallet);
      await this.publishWagerProcessed(processed, wallet, applyResult.entry);
      return processed.status;
    });
  }

  private async executeWithinTransaction(input: ProcessWagerInput): Promise<ProcessWagerResult> {
    // This use case is the single entry point for BOTH HTTP and SQS (spec §10), so kinds that
    // may never be submitted from outside (OPENING, which only CreateWalletUseCase mints) are
    // rejected here — once — instead of in each adapter, where one of them would inevitably
    // forget. Without this, an SQS envelope with kind "OPENING" would be credited as real money.
    if (!isExternallySubmittableKind(input.kind)) {
      return this.rejectNew(input, FailureCode.INVALID_KIND);
    }

    const existing = await this.wagerTransactionRepository.findByProviderAndExternalId(
      input.providerId,
      input.externalTransactionId,
    );
    if (existing) {
      return this.toResult(existing, true);
    }

    // The wallet row is the unit of concurrency. Lock it before inspecting reversal state so
    // two distinct REFUND/ROLLBACK requests cannot both observe "not reversed" and credit it.
    let wallet = await this.walletRepository.findByIdForUpdate(input.walletId);
    if (!wallet) {
      throw new NotFoundException(`Wallet ${input.walletId} not found`);
    }
    if (input.playerId && wallet.playerId !== input.playerId) {
      return this.rejectNew(input, FailureCode.PLAYER_MISMATCH);
    }

    let referencedTx: WagerTransaction | null = null;

    if (input.kind === "REFUND" || input.kind === "ROLLBACK") {
      if (!input.referenceExternalTransactionId) {
        return this.rejectNew(input, FailureCode.REFERENCE_NOT_FOUND);
      }

      referencedTx = await this.wagerTransactionRepository.findByProviderAndExternalId(
        input.providerId,
        input.referenceExternalTransactionId,
      );

      if (!referencedTx || referencedTx.status !== "PROCESSED") {
        const tx = this.newTransaction(input).markPendingReference();
        await this.wagerTransactionRepository.save(tx);
        await this.publishPendingReference(tx);
        return { status: "PENDING_REFERENCE", transactionId: tx.id, idempotentReplay: false };
      }

      const alreadyReversed =
        (await this.wagerTransactionRepository.findProcessedReversalFor(
          input.providerId,
          input.referenceExternalTransactionId,
        )) !== null;

      const rejection = reversalRejection({
        kind: input.kind,
        submittedWalletId: input.walletId,
        referencedKind: referencedTx.kind,
        referencedWalletId: referencedTx.walletId,
        submittedPlayerId: input.playerId ?? referencedTx.playerId,
        referencedPlayerId: referencedTx.playerId,
        submittedRoundId: input.roundId ?? referencedTx.roundId,
        referencedRoundId: referencedTx.roundId,
        submittedAmount: input.amount,
        referencedAmount: referencedTx.amount,
        alreadyReversed,
      });
      if (rejection) {
        return this.rejectNew(input, rejection);
      }
    }

    const tx = this.newTransaction(input);
    if (input.kind === "LOSS") {
      const processedTx = tx.markProcessed(wallet.balance);
      await this.wagerTransactionRepository.save(processedTx);
      await this.publishWagerProcessed(processedTx, wallet);
      wallet = await this.resolvePendingReferences(wallet, processedTx);
      return { status: "PROCESSED", transactionId: tx.id, balance: wallet.balance, idempotentReplay: false };
    }

    const effectiveAmount = referencedTx ? referencedTx.amount : input.amount;
    const applyResult = this.applyToWallet(wallet, input.kind, referencedTx?.kind ?? null, effectiveAmount, tx.id);

    if (applyResult.status === "REJECTED") {
      const failureCode = referencedTx && applyResult.failureCode === FailureCode.INSUFFICIENT_FUNDS
        ? FailureCode.REVERSAL_WOULD_MAKE_BALANCE_NEGATIVE
        : applyResult.failureCode;
      await this.wagerTransactionRepository.save(tx.markRejected(failureCode));
      await this.publishRejected(tx.markRejected(failureCode));
      return {
        status: "REJECTED",
        transactionId: tx.id,
        failureCode,
        idempotentReplay: false,
      };
    }

    wallet = applyResult.wallet;
    const processedTx = tx.markProcessed(wallet.balance, referencedTx?.id ?? null);
    await this.wagerTransactionRepository.save(processedTx);
    // wallet_ledger_entries.transaction_id has an FK into wager_transactions, and that
    // relationship is not expressed to MikroORM (plain UUID columns, no relation), so the
    // unit of work has no dependency graph telling it to insert the transaction row first —
    // flush explicitly before persisting the ledger entry that references it.
    await this.em.flush();
    await this.walletRepository.appendLedgerEntry(applyResult.entry);
    await this.walletRepository.save(wallet);
    await this.publishWagerProcessed(processedTx, wallet, applyResult.entry);

    wallet = await this.resolvePendingReferences(wallet, processedTx);

    return { status: "PROCESSED", transactionId: tx.id, balance: wallet.balance, idempotentReplay: false };
  }

  /** Persists a brand-new transaction row in REJECTED state and returns the rejection result. */
  private async rejectNew(input: ProcessWagerInput, failureCode: FailureCode): Promise<ProcessWagerResult> {
    const tx = this.newTransaction(input).markRejected(failureCode);
    await this.wagerTransactionRepository.save(tx);
    await this.publishRejected(tx);
    return { status: "REJECTED", transactionId: tx.id, failureCode, idempotentReplay: false };
  }

  private newTransaction(input: ProcessWagerInput): WagerTransaction {
    return WagerTransaction.create({
      id: crypto.randomUUID(),
      walletId: input.walletId,
      externalTransactionId: input.externalTransactionId,
      providerId: input.providerId,
      playerId: input.playerId ?? "legacy-player",
      roundId: input.roundId ?? "legacy-round",
      gameId: input.gameId ?? "legacy-game",
      idempotencyKey: input.idempotencyKey,
      payloadHash: input.payloadHash,
      kind: input.kind,
      amount: input.amount,
      referenceExternalTransactionId: input.referenceExternalTransactionId,
    });
  }

  private applyToWallet(
    wallet: Wallet,
    kind: WagerKind,
    referencedKind: WagerKind | null,
    amount: Money,
    transactionId: string,
  ): WalletApplyResult {
    const isDebit = referencedKind
      ? originalDirectionOf(referencedKind) === "CREDIT" // a reversal flips the original direction
      : originalDirectionOf(kind) === "DEBIT";
    return isDebit ? wallet.debit({ amount, transactionId }) : wallet.credit({ amount, transactionId });
  }

  private wagerEventData(tx: WagerTransaction, balance?: Money): WagerEventData {
    return {
      transactionId: tx.id,
      walletId: tx.walletId,
      playerId: tx.playerId,
      providerId: tx.providerId,
      externalTransactionId: tx.externalTransactionId,
      roundId: tx.roundId,
      gameId: tx.gameId,
      kind: tx.kind,
      money: tx.amount.toJSON(),
      ...(balance ? { balance: balance.toJSON() } : {}),
      ...(tx.failureCode ? { failureCode: tx.failureCode } : {}),
      ...(tx.referenceExternalTransactionId
        ? { referenceExternalTransactionId: tx.referenceExternalTransactionId }
        : {}),
    };
  }

  private async appendEvent(event: IntegrationEvent<unknown>): Promise<void> {
    await this.outboxRepository.append({
      id: event.eventId,
      aggregateId: event.aggregateId,
      eventType: event.eventType,
      payload: event.toJSON(),
      occurredAt: event.occurredAt,
    });
  }

  private async publishWagerProcessed(
    tx: WagerTransaction,
    wallet: Wallet,
    entry?: WalletLedgerEntry,
  ): Promise<void> {
    await this.appendEvent(new WagerTransactionProcessed({
      aggregateId: wallet.id,
      correlationId: tx.idempotencyKey,
      causationId: tx.referenceTransactionId ?? undefined,
      data: this.wagerEventData(tx, wallet.balance),
    }));
    if (entry) {
      await this.appendEvent(new WalletBalanceChanged({
        aggregateId: wallet.id,
        correlationId: tx.idempotencyKey,
        causationId: tx.id,
        data: {
          walletId: wallet.id,
          transactionId: tx.id,
          direction: entry.direction,
          money: entry.amount.toJSON(),
          balanceBefore: entry.balanceBefore.toJSON(),
          balanceAfter: entry.balanceAfter.toJSON(),
          walletVersion: wallet.version,
        },
      }));
    }
  }

  private async publishRejected(tx: WagerTransaction): Promise<void> {
    await this.appendEvent(new WagerTransactionRejected({
      aggregateId: tx.walletId,
      correlationId: tx.idempotencyKey,
      data: this.wagerEventData(tx),
    }));
  }

  private async publishPendingReference(tx: WagerTransaction): Promise<void> {
    await this.appendEvent(new WagerTransactionPendingReference({
      aggregateId: tx.walletId,
      correlationId: tx.idempotencyKey,
      data: this.wagerEventData(tx),
    }));
  }

  /** Resolves REFUND/ROLLBACK rows that were waiting on the transaction just processed. */
  private async resolvePendingReferences(wallet: Wallet, justProcessed: WagerTransaction): Promise<Wallet> {
    const pendingRows = await this.wagerTransactionRepository.findPendingReferencesFor(
      justProcessed.providerId,
      justProcessed.externalTransactionId,
    );
    let currentWallet = wallet;
    // `currentWallet` is the wallet the just-processed transaction belongs to and the only wallet
    // this method ever credits; a pending row whose own walletId disagrees with it is rejected
    // (WALLET_MISMATCH) rather than silently redirected. Seeded from the DB in case a reversal was
    // somehow already applied, then flipped locally: at most ONE pending reversal per referenced
    // transaction may be credited, every later one is REFERENCE_ALREADY_REVERSED. (A local flag,
    // not a re-query: `save()` only calls `em.persist()`, so a reversal marked PROCESSED earlier in
    // this loop is not yet visible to `findProcessedReversalFor`'s SELECT until the next flush —
    // and even after a flush, MikroORM's identity map could return the stale row.)
    let alreadyReversed =
      (await this.wagerTransactionRepository.findProcessedReversalFor(
        justProcessed.providerId,
        justProcessed.externalTransactionId,
      )) !== null;
    for (const pending of pendingRows) {
      const rejection = reversalRejection({
        kind: pending.kind,
        submittedWalletId: pending.walletId,
        referencedKind: justProcessed.kind,
        referencedWalletId: currentWallet.id,
        submittedPlayerId: pending.playerId,
        referencedPlayerId: justProcessed.playerId,
        submittedRoundId: pending.roundId,
        referencedRoundId: justProcessed.roundId,
        submittedAmount: pending.amount,
        referencedAmount: justProcessed.amount,
        alreadyReversed,
      });
      if (rejection) {
        const rejected = pending.markRejected(rejection);
        await this.wagerTransactionRepository.save(rejected);
        await this.publishRejected(rejected);
        continue;
      }
      const applyResult = this.applyToWallet(
        currentWallet,
        pending.kind,
        justProcessed.kind,
        justProcessed.amount,
        pending.id,
      );
      if (applyResult.status === "REJECTED") {
        const failureCode = applyResult.failureCode === FailureCode.INSUFFICIENT_FUNDS
          ? FailureCode.REVERSAL_WOULD_MAKE_BALANCE_NEGATIVE
          : applyResult.failureCode;
        const rejected = pending.markRejected(failureCode);
        await this.wagerTransactionRepository.save(rejected);
        await this.publishRejected(rejected);
        continue;
      }
      currentWallet = applyResult.wallet;
      alreadyReversed = true;
      const processedPending = pending.markProcessed(currentWallet.balance, justProcessed.id);
      await this.wagerTransactionRepository.save(processedPending);
      await this.em.flush();
      await this.walletRepository.appendLedgerEntry(applyResult.entry);
      await this.walletRepository.save(currentWallet);
      await this.publishWagerProcessed(processedPending, currentWallet, applyResult.entry);
    }
    return currentWallet;
  }

  private toResult(tx: WagerTransaction, idempotentReplay: boolean): ProcessWagerResult {
    if (tx.status === "PROCESSED") {
      return { status: "PROCESSED", transactionId: tx.id, balance: tx.resultBalance!, idempotentReplay };
    }
    if (tx.status === "PENDING_REFERENCE") {
      return { status: "PENDING_REFERENCE", transactionId: tx.id, idempotentReplay };
    }
    return { status: "REJECTED", transactionId: tx.id, failureCode: tx.failureCode!, idempotentReplay };
  }
}
