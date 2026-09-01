import { IsIn, IsNotEmpty, IsOptional, IsString, Length } from "class-validator";

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

  @IsIn(SUBMITTABLE_KINDS)
  kind!: (typeof SUBMITTABLE_KINDS)[number];

  @IsString()
  @IsNotEmpty()
  amount!: string;

  @IsString()
  @Length(3, 3)
  currency!: string;

  @IsOptional()
  @IsString()
  referenceExternalTransactionId?: string;
}
