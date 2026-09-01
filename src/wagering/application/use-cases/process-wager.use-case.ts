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
import { type WagerKind, WagerTransaction, validateReferenceKind } from "../../domain/wager-transaction.js";
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
        const tx = this.newTransaction(input).markRejected(FailureCode.REFERENCE_NOT_FOUND);
        await this.wagerTransactionRepository.save(tx);
        return {
          status: "REJECTED",
          transactionId: tx.id,
          failureCode: FailureCode.REFERENCE_NOT_FOUND,
          idempotentReplay: false,
        };
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

      const invalidRefCode = validateReferenceKind(input.kind, referencedTx.kind);
      if (invalidRefCode) {
        const tx = this.newTransaction(input).markRejected(invalidRefCode);
        await this.wagerTransactionRepository.save(tx);
        return { status: "REJECTED", transactionId: tx.id, failureCode: invalidRefCode, idempotentReplay: false };
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
    for (const pending of pendingRows) {
      const invalidRefCode = validateReferenceKind(pending.kind, justProcessed.kind);
      if (invalidRefCode) {
        await this.wagerTransactionRepository.save(pending.markRejected(invalidRefCode));
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
