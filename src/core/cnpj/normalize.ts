import {
  CNPJ_ASCII_LOWERCASE,
  CNPJ_BASE_PATTERN,
  CNPJ_BASE_LENGTH,
  CNPJ_DV_PATTERN,
  CNPJ_LENGTH,
  CNPJ_MASK_PATTERN,
  CNPJ_MASK_SEPARATOR,
  CNPJ_MASK_SEPARATOR_GLOBAL,
  CNPJ_NORMALIZATION_POLICY,
} from "./constants.js";
import { cnpjFailure } from "./errors.js";
import type {
  CnpjCanonical,
  CnpjNormalizeOptions,
  CnpjResult,
} from "./types.js";

export function normalizeCnpj(
  input: string | null | undefined,
  options?: CnpjNormalizeOptions,
): CnpjResult {
  if (input == null) {
    return cnpjFailure("CNPJ_EMPTY");
  }

  const trimmed = input.trim();
  if (trimmed === "") {
    return cnpjFailure("CNPJ_EMPTY");
  }

  const acceptLowercase =
    options?.acceptLowercase ?? CNPJ_NORMALIZATION_POLICY.acceptLowercase;
  const acceptStandardMask =
    options?.acceptStandardMask ?? CNPJ_NORMALIZATION_POLICY.acceptStandardMask;

  const cased = acceptLowercase
    ? trimmed.replace(CNPJ_ASCII_LOWERCASE, (char) => char.toUpperCase())
    : trimmed;

  let bare: string;
  if (CNPJ_MASK_SEPARATOR.test(cased)) {
    if (!acceptStandardMask || !CNPJ_MASK_PATTERN.test(cased)) {
      return cnpjFailure("CNPJ_INVALID_MASK");
    }

    bare = cased.replace(CNPJ_MASK_SEPARATOR_GLOBAL, "");
  } else {
    bare = cased;
  }

  if (bare.length !== CNPJ_LENGTH) {
    return cnpjFailure("CNPJ_INVALID_LENGTH");
  }

  const base = bare.slice(0, CNPJ_BASE_LENGTH);
  const dv = bare.slice(CNPJ_BASE_LENGTH);

  if (!CNPJ_BASE_PATTERN.test(base)) {
    return cnpjFailure("CNPJ_INVALID_CHARSET");
  }

  if (!CNPJ_DV_PATTERN.test(dv)) {
    return cnpjFailure("CNPJ_INVALID_DV_CHARSET");
  }

  return { ok: true, value: bare as CnpjCanonical };
}
