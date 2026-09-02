import { Inject, Injectable, NotFoundException } from "@nestjs/common";
import { EntityManager, UniqueConstraintViolationException } from "@mikro-orm/postgresql";
import { Money } from "../../../shared-kernel/money.js";
import { FailureCode } from "../../../shared-kernel/failure-code.js";
import { Wallet, type WalletApplyResult } from "../../../wallet/domain/wallet.js";
import {
  WALLET_REPOSITORY,
  type WalletRepository,
} from "../../../wallet/application/ports/wallet.repository.js";
import { OUTBOX_REPOSITORY, type OutboxRepository } from "../../../messaging/application/ports/outbox.repository.js";
import {
  type WagerKind,
  WagerTransaction,
  isExternallySubmittableKind,
  validateReferenceKind,
  validateReversalWallet,
} from "../../domain/wager-transaction.js";
import {
  WAGER_TRANSACTION_REPOSITORY,
  type WagerTransactionRepository,
} from "../ports/wager-transaction.repository.js";

export interface ProcessWagerInput {
  externalTransactionId: string;
  providerId: string;
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
  return kind === "BET" || kind === "LOSS" ? "DEBIT" : "CREDIT";
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
  alreadyReversed: boolean;
}): FailureCode | null {
  return (
    validateReferenceKind(input.kind, input.referencedKind) ??
    validateReversalWallet(input.submittedWalletId, input.referencedWalletId) ??
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
        alreadyReversed,
      });
      if (rejection) {
        return this.rejectNew(input, rejection);
      }
    }

    let wallet = await this.walletRepository.findByIdForUpdate(input.walletId);
    if (!wallet) {
      throw new NotFoundException(`Wallet ${input.walletId} not found`);
    }

    const tx = this.newTransaction(input);
    const effectiveAmount = referencedTx ? referencedTx.amount : input.amount;
    const applyResult = this.applyToWallet(wallet, input.kind, referencedTx?.kind ?? null, effectiveAmount, tx.id);

    if (applyResult.status === "REJECTED") {
      await this.wagerTransactionRepository.save(tx.markRejected(applyResult.failureCode));
      return {
        status: "REJECTED",
        transactionId: tx.id,
        failureCode: applyResult.failureCode,
        idempotentReplay: false,
      };
    }

    wallet = applyResult.wallet;
    const processedTx = tx.markProcessed(wallet.balance);
    await this.wagerTransactionRepository.save(processedTx);
    // wallet_ledger_entries.transaction_id has an FK into wager_transactions, and that
    // relationship is not expressed to MikroORM (plain UUID columns, no relation), so the
    // unit of work has no dependency graph telling it to insert the transaction row first —
    // flush explicitly before persisting the ledger entry that references it.
    await this.em.flush();
    await this.walletRepository.appendLedgerEntry(applyResult.entry);
    await this.walletRepository.save(wallet);
    await this.publishWagerProcessed(processedTx, wallet);

    wallet = await this.resolvePendingReferences(wallet, processedTx);

    return { status: "PROCESSED", transactionId: tx.id, balance: wallet.balance, idempotentReplay: false };
  }

  /** Persists a brand-new transaction row in REJECTED state and returns the rejection result. */
  private async rejectNew(input: ProcessWagerInput, failureCode: FailureCode): Promise<ProcessWagerResult> {
    const tx = this.newTransaction(input).markRejected(failureCode);
    await this.wagerTransactionRepository.save(tx);
    return { status: "REJECTED", transactionId: tx.id, failureCode, idempotentReplay: false };
  }

  private newTransaction(input: ProcessWagerInput): WagerTransaction {
    return WagerTransaction.create({
      id: crypto.randomUUID(),
      walletId: input.walletId,
      externalTransactionId: input.externalTransactionId,
      providerId: input.providerId,
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

  private async publishWagerProcessed(tx: WagerTransaction, wallet: Wallet): Promise<void> {
    await this.outboxRepository.append({
      id: crypto.randomUUID(),
      aggregateId: wallet.id,
      eventType: "WagerProcessed",
      payload: {
        transactionId: tx.id,
        walletId: wallet.id,
        kind: tx.kind,
        balance: wallet.balance.toJSON(),
      },
      occurredAt: new Date(),
    });
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
        alreadyReversed,
      });
      if (rejection) {
        await this.wagerTransactionRepository.save(pending.markRejected(rejection));
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
        await this.wagerTransactionRepository.save(pending.markRejected(applyResult.failureCode));
        continue;
      }
      currentWallet = applyResult.wallet;
      alreadyReversed = true;
      await this.wagerTransactionRepository.save(pending.markProcessed(currentWallet.balance));
      await this.em.flush();
      await this.walletRepository.appendLedgerEntry(applyResult.entry);
      await this.walletRepository.save(currentWallet);
      await this.publishWagerProcessed(pending.markProcessed(currentWallet.balance), currentWallet);
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
