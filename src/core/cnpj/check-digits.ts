import {
  CNPJ_BASE_PATTERN,
  CNPJ_FIRST_DV_WEIGHTS,
  CNPJ_SECOND_DV_WEIGHTS,
} from "./constants.js";
import { CnpjError } from "./errors.js";

function charDvValue(char: string): number {
  return char.charCodeAt(0) - 48;
}

function computeSingleDv(
  values: readonly number[],
  weights: readonly number[],
): number {
  let sum = 0;

  for (let index = 0; index < weights.length; index += 1) {
    const value = values[index];
    const weight = weights[index];

    if (value === undefined || weight === undefined) {
      throw new CnpjError("CNPJ_INVALID_CHARSET");
    }

    sum += value * weight;
  }

  const rest = sum % 11;
  return rest < 2 ? 0 : 11 - rest;
}

export function computeCheckDigits(base: string): string {
  if (!CNPJ_BASE_PATTERN.test(base)) {
    throw new CnpjError("CNPJ_INVALID_CHARSET");
  }

  const values = Array.from(base, charDvValue);
  const firstDv = computeSingleDv(values, CNPJ_FIRST_DV_WEIGHTS);
  const secondDv = computeSingleDv(
    [...values, firstDv],
    CNPJ_SECOND_DV_WEIGHTS,
  );

  return `${firstDv}${secondDv}`;
}
