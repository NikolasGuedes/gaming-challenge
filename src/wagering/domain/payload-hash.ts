import { createHash } from "node:crypto";

function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(canonicalize);
  }
  if (value !== null && typeof value === "object") {
    const sorted: Record<string, unknown> = {};
    for (const key of Object.keys(value as Record<string, unknown>).sort()) {
      sorted[key] = canonicalize((value as Record<string, unknown>)[key]);
    }
    return sorted;
  }
  return value;
}

/** Canonical JSON (sorted keys) sha256 hash of the business-relevant subset of a payload. */
export function computePayloadHash(businessFields: Record<string, unknown>): string {
  const canonicalJson = JSON.stringify(canonicalize(businessFields));
  return createHash("sha256").update(canonicalJson).digest("hex");
}
