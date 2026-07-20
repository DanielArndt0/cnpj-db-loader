import { CNPJ_CANONICAL_PATTERN, CNPJ_LENGTH } from "./constants.js";
import { CnpjError } from "./errors.js";
import { normalizeCnpj } from "./normalize.js";
import type { CnpjCanonical } from "./types.js";

export function formatCnpj(canonical: string): string {
  if (canonical.length !== CNPJ_LENGTH) {
    throw new CnpjError("CNPJ_INVALID_LENGTH");
  }

  if (!CNPJ_CANONICAL_PATTERN.test(canonical)) {
    throw new CnpjError("CNPJ_INVALID_CHARSET");
  }

  return (
    `${canonical.slice(0, 2)}.${canonical.slice(2, 5)}.${canonical.slice(5, 8)}` +
    `/${canonical.slice(8, 12)}-${canonical.slice(12, 14)}`
  );
}

export function stripCnpjMask(masked: string): CnpjCanonical {
  const result = normalizeCnpj(masked);
  if (!result.ok) {
    throw new CnpjError(result.code, result.message);
  }

  return result.value;
}
