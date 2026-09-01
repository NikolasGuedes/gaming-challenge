import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Headers,
  Inject,
  NotFoundException,
  Param,
  Post,
} from "@nestjs/common";
import { Money } from "../../../shared-kernel/money.js";
import { computePayloadHash } from "../../domain/payload-hash.js";
import { ProcessWagerUseCase, ProcessWagerResult } from "../../application/use-cases/process-wager.use-case.js";
import {
  WAGER_TRANSACTION_REPOSITORY,
  type WagerTransactionRepository,
} from "../../application/ports/wager-transaction.repository.js";
import { WagerTransaction } from "../../domain/wager-transaction.js";
import { IdempotencyService } from "./idempotency.service.js";
import { SubmitWagerDto } from "./dto/submit-wager.dto.js";
import { toWagerResponseDto, WagerResponseDto } from "./dto/wager-response.dto.js";

@Controller()
export class WageringController {
  constructor(
    private readonly processWagerUseCase: ProcessWagerUseCase,
    private readonly idempotencyService: IdempotencyService,
    @Inject(WAGER_TRANSACTION_REPOSITORY)
    private readonly wagerTransactionRepository: WagerTransactionRepository,
  ) {}

  @Post("wagering/transactions")
  async submit(
    @Headers("idempotency-key") idempotencyKey: string | undefined,
    @Body() dto: SubmitWagerDto,
  ): Promise<WagerResponseDto> {
    if (!idempotencyKey) {
      throw new BadRequestException("Idempotency-Key header is required");
    }

    const businessFields = {
      externalTransactionId: dto.externalTransactionId,
      providerId: dto.providerId,
      walletId: dto.walletId,
      kind: dto.kind,
      amount: dto.amount,
      currency: dto.currency,
      referenceExternalTransactionId: dto.referenceExternalTransactionId ?? null,
    };
    const payloadHash = computePayloadHash(businessFields);

    // The cached response is round-tripped through a jsonb column (see
    // IdempotencyKeyEntity#response), so anything stored there comes back as plain JSON, not
    // domain objects (e.g. Money). Serialize to WagerResponseDto *inside* the executed closure,
    // before it is handed to the idempotency cache, so both the first-call and replay paths
    // return the same plain-JSON shape.
    const { response, idempotentReplay } = await this.idempotencyService.handle(
      idempotencyKey,
      businessFields,
      async () => {
        const result = await this.processWagerUseCase.execute({
          externalTransactionId: dto.externalTransactionId,
          providerId: dto.providerId,
          idempotencyKey,
          payloadHash,
          kind: dto.kind,
          walletId: dto.walletId,
          amount: Money.from({ amount: dto.amount, currency: dto.currency }),
          referenceExternalTransactionId: dto.referenceExternalTransactionId ?? null,
        });
        return toWagerResponseDto(result, result.idempotentReplay);
      },
    );

    return { ...response, idempotentReplay: idempotentReplay || response.idempotentReplay };
  }

  @Get("wagering/transactions/:transactionId")
  async getByTransactionId(@Param("transactionId") transactionId: string): Promise<WagerResponseDto> {
    const tx = await this.wagerTransactionRepository.findById(transactionId);
    if (!tx) {
      throw new NotFoundException(`Transaction ${transactionId} not found`);
    }
    return toWagerResponseDto(this.toProcessResult(tx), false);
  }

  @Get("providers/:providerId/wagering/transactions/:externalTransactionId")
  async getByExternalId(
    @Param("providerId") providerId: string,
    @Param("externalTransactionId") externalTransactionId: string,
  ): Promise<WagerResponseDto> {
    const tx = await this.wagerTransactionRepository.findByProviderAndExternalId(providerId, externalTransactionId);
    if (!tx) {
      throw new NotFoundException(`Transaction ${externalTransactionId} for provider ${providerId} not found`);
    }
    return toWagerResponseDto(this.toProcessResult(tx), false);
  }

  private toProcessResult(tx: WagerTransaction): ProcessWagerResult {
    if (tx.status === "PROCESSED") {
      return { status: "PROCESSED", transactionId: tx.id, balance: tx.resultBalance!, idempotentReplay: false };
    }
    if (tx.status === "PENDING_REFERENCE") {
      return { status: "PENDING_REFERENCE", transactionId: tx.id, idempotentReplay: false };
    }
    return {
      status: "REJECTED",
      transactionId: tx.id,
      failureCode: tx.failureCode!,
      idempotentReplay: false,
    };
  }
}
