import { ProcessWagerResult } from "../../../application/use-cases/process-wager.use-case.js";

export interface WagerResponseDto {
  transactionId: string;
  status: "PROCESSED" | "REJECTED" | "PENDING_REFERENCE";
  idempotentReplay: boolean;
  balance?: { amount: string; currency: string };
  failureCode?: string;
}

export function toWagerResponseDto(result: ProcessWagerResult, idempotentReplay: boolean): WagerResponseDto {
  const base = { transactionId: result.transactionId, status: result.status, idempotentReplay };
  if (result.status === "PROCESSED") {
    return { ...base, balance: result.balance.toJSON() };
  }
  if (result.status === "REJECTED") {
    return { ...base, failureCode: result.failureCode };
  }
  return base;
}
