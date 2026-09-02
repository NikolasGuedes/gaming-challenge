import { IntegrationEvent, IntegrationEventProps } from "../../shared-kernel/integration-event.js";
import { LedgerDirection } from "../../wallet/domain/wallet-ledger-entry.js";
import { WagerKind } from "./wager-transaction.js";

export interface WagerEventData {
  transactionId: string;
  walletId: string;
  playerId: string;
  providerId: string;
  externalTransactionId: string;
  roundId: string;
  gameId: string;
  kind: WagerKind;
  money: { amount: string; currency: string };
  balance?: { amount: string; currency: string };
  failureCode?: string;
  referenceExternalTransactionId?: string;
}

abstract class WagerIntegrationEvent extends IntegrationEvent<WagerEventData> {
  readonly version = 1;
}

export class WagerTransactionProcessed extends WagerIntegrationEvent {
  readonly eventType = "WagerTransactionProcessed";
  constructor(props: IntegrationEventProps<WagerEventData>) { super(props); }
}

export class WagerTransactionRejected extends WagerIntegrationEvent {
  readonly eventType = "WagerTransactionRejected";
  constructor(props: IntegrationEventProps<WagerEventData>) { super(props); }
}

export class WagerTransactionPendingReference extends WagerIntegrationEvent {
  readonly eventType = "WagerTransactionPendingReference";
  constructor(props: IntegrationEventProps<WagerEventData>) { super(props); }
}

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: { amount: string; currency: string };
  balanceBefore: { amount: string; currency: string };
  balanceAfter: { amount: string; currency: string };
  walletVersion: number;
}

export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = "WalletBalanceChanged";
  readonly version = 1;
  constructor(props: IntegrationEventProps<WalletBalanceChangedData>) { super(props); }
}
