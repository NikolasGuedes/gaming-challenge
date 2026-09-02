import { describe, expect, it } from "bun:test";
import { computePayloadHash } from "./payload-hash.js";

describe("computePayloadHash", () => {
  it("is stable regardless of key order", () => {
    const a = computePayloadHash({ amount: "25.00", currency: "BRL", walletId: "w1" });
    const b = computePayloadHash({ walletId: "w1", currency: "BRL", amount: "25.00" });
    expect(a).toBe(b);
  });

  it("differs when a value differs", () => {
    const a = computePayloadHash({ amount: "25.00", currency: "BRL" });
    const b = computePayloadHash({ amount: "25.01", currency: "BRL" });
    expect(a).not.toBe(b);
  });

  it("produces a 64-char hex sha256 digest", () => {
    const hash = computePayloadHash({ amount: "25.00" });
    expect(hash).toMatch(/^[0-9a-f]{64}$/);
  });
});
