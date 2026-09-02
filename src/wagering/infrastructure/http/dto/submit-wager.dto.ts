import { Type } from "class-transformer";
import { IsIn, IsNotEmpty, IsOptional, IsString, ValidateNested } from "class-validator";
import { MoneyDto } from "../../../../shared-kernel/http/money.dto.js";

const SUBMITTABLE_KINDS = ["BET", "WIN", "LOSS", "REFUND", "ROLLBACK"] as const;

export class SubmitWagerDto {
  @IsString()
  @IsNotEmpty()
  externalTransactionId!: string;

  @IsString()
  @IsNotEmpty()
  providerId!: string;

  @IsString()
  @IsNotEmpty()
  walletId!: string;

  @IsString()
  @IsNotEmpty()
  playerId!: string;

  @IsString()
  @IsNotEmpty()
  roundId!: string;

  @IsString()
  @IsNotEmpty()
  gameId!: string;

  @IsIn(SUBMITTABLE_KINDS)
  kind!: (typeof SUBMITTABLE_KINDS)[number];

  @ValidateNested()
  @Type(() => MoneyDto)
  money!: MoneyDto;

  @IsOptional()
  @IsString()
  referenceExternalTransactionId?: string;
}
