export class InvalidMoneyError extends Error {
  constructor(reason: string, input: unknown) {
    super(`Invalid money value: ${reason} (received ${JSON.stringify(input)})`);
    this.name = "InvalidMoneyError";
  }
}

export class CurrencyMismatchError extends Error {
  constructor(left: string, right: string) {
    super(`Currency mismatch: ${left} vs ${right}`);
    this.name = "CurrencyMismatchError";
  }
}
