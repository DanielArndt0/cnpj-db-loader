import { computeCheckDigits } from "./check-digits.js";
import { CNPJ_BASE_LENGTH } from "./constants.js";
import { cnpjFailure, CnpjError } from "./errors.js";
import { normalizeCnpj } from "./normalize.js";
import type {
  CnpjCanonical,
  CnpjNormalizeOptions,
  CnpjResult,
} from "./types.js";

export function validateCnpj(
  input: string | null | undefined,
  options?: CnpjNormalizeOptions,
): CnpjResult {
  const normalized = normalizeCnpj(input, options);
  if (!normalized.ok) {
    return normalized;
  }

  const canonical = normalized.value;
  const expected = computeCheckDigits(canonical.slice(0, CNPJ_BASE_LENGTH));

  if (canonical.slice(CNPJ_BASE_LENGTH) !== expected) {
    return cnpjFailure("CNPJ_INVALID_CHECK_DIGITS");
  }

  return normalized;
}

export function isValidCnpj(
  input: string | null | undefined,
  options?: CnpjNormalizeOptions,
): boolean {
  return validateCnpj(input, options).ok;
}

export function assertCnpj(
  input: string | null | undefined,
  options?: CnpjNormalizeOptions,
): CnpjCanonical {
  const result = validateCnpj(input, options);
  if (!result.ok) {
    throw new CnpjError(result.code, result.message);
  }

  return result.value;
}
