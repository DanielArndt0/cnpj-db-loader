import {
  CNPJ_BASICO_LENGTH,
  CNPJ_BASICO_PATTERN,
  CNPJ_BASE_LENGTH,
  CNPJ_CANONICAL_PATTERN,
  CNPJ_DV_PATTERN,
  CNPJ_ORDEM_PATTERN,
} from "./constants.js";
import { CnpjError } from "./errors.js";
import type { CnpjCanonical, CnpjSegments } from "./types.js";

export function composeCnpjCompleto(
  basico: string,
  ordem: string,
  dv: string,
): CnpjCanonical {
  if (!CNPJ_BASICO_PATTERN.test(basico)) {
    throw new CnpjError(
      "CNPJ_INVALID_CHARSET",
      "CNPJ básico inválido: são esperados 8 caracteres A-Z0-9.",
    );
  }

  if (!CNPJ_ORDEM_PATTERN.test(ordem)) {
    throw new CnpjError(
      "CNPJ_INVALID_CHARSET",
      "Ordem do estabelecimento inválida: são esperados 4 caracteres A-Z0-9.",
    );
  }

  if (!CNPJ_DV_PATTERN.test(dv)) {
    throw new CnpjError(
      "CNPJ_INVALID_DV_CHARSET",
      "Dígitos verificadores inválidos: são esperados 2 dígitos 0-9.",
    );
  }

  return `${basico}${ordem}${dv}` as CnpjCanonical;
}

export function splitCnpj(completo: string): CnpjSegments {
  if (!CNPJ_CANONICAL_PATTERN.test(completo)) {
    throw new CnpjError("CNPJ_INVALID_CHARSET");
  }

  return {
    basico: completo.slice(0, CNPJ_BASICO_LENGTH),
    ordem: completo.slice(CNPJ_BASICO_LENGTH, CNPJ_BASE_LENGTH),
    dv: completo.slice(CNPJ_BASE_LENGTH),
  };
}
