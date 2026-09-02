import { IsNotEmpty, IsString, Length, Matches } from "class-validator";

export class MoneyDto {
  @IsString()
  @IsNotEmpty()
  @Matches(/^\d+(?:\.\d{1,2})?$/, { message: "amount must be a non-negative decimal string with at most 2 decimal places" })
  amount!: string;

  @IsString()
  @Length(3, 3)
  currency!: string;
}
