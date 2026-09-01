import { BadRequestException, Body, Controller, Headers, Post } from "@nestjs/common";
import { Money } from "../../../shared-kernel/money.js";
import { computePayloadHash } from "../../domain/payload-hash.js";
import { ProcessWagerUseCase } from "../../application/use-cases/process-wager.use-case.js";
import { IdempotencyService } from "./idempotency.service.js";
import { SubmitWagerDto } from "./dto/submit-wager.dto.js";
import { toWagerResponseDto, WagerResponseDto } from "./dto/wager-response.dto.js";

@Controller("wagering")
export class WageringController {
  constructor(
    private readonly processWagerUseCase: ProcessWagerUseCase,
    private readonly idempotencyService: IdempotencyService,
  ) {}

  @Post("transactions")
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
}
