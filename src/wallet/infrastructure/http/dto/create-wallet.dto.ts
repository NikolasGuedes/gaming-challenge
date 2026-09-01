import { IsNotEmpty, IsOptional, IsString, Length } from "class-validator";

export class CreateWalletDto {
  @IsString()
  @IsNotEmpty()
  playerId!: string;

  @IsString()
  @Length(3, 3)
  currency!: string;

  @IsOptional()
  @IsString()
  initialBalance?: string;
}
