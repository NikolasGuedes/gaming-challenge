import { ReconciliationResult } from "../../../application/use-cases/reconcile-wallet.use-case.js";

export interface ReconciliationResponseDto {
  walletId: string;
  storedBalance: { amount: string; currency: string };
  calculatedBalance: { amount: string; currency: string };
  difference: { amount: string; currency: string };
  consistent: boolean;
  checkedEntries: number;
}

export function toReconciliationResponseDto(result: ReconciliationResult): ReconciliationResponseDto {
  return {
    walletId: result.walletId,
    storedBalance: result.storedBalance.toJSON(),
    calculatedBalance: result.calculatedBalance.toJSON(),
    difference: result.difference.toJSON(),
    consistent: result.consistent,
    checkedEntries: result.checkedEntries,
  };
}
