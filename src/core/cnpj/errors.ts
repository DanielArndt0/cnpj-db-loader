import { AppError } from "../errors/app-error.js";

import type { CnpjFailure } from "./types.js";

export type CnpjErrorCode =
  | "CNPJ_EMPTY"
  | "CNPJ_INVALID_MASK"
  | "CNPJ_INVALID_LENGTH"
  | "CNPJ_INVALID_CHARSET"
  | "CNPJ_INVALID_DV_CHARSET"
  | "CNPJ_INVALID_CHECK_DIGITS";

export const CNPJ_ERROR_MESSAGES: Record<CnpjErrorCode, string> = {
  CNPJ_EMPTY: "CNPJ não informado.",
  CNPJ_INVALID_MASK:
    "Máscara de CNPJ inválida. Use o formato 00.000.000/0000-00.",
  CNPJ_INVALID_LENGTH:
    "CNPJ deve conter exatamente 14 caracteres após a normalização.",
  CNPJ_INVALID_CHARSET:
    "As 12 primeiras posições do CNPJ aceitam apenas os caracteres A-Z e 0-9.",
  CNPJ_INVALID_DV_CHARSET:
    "Os dois dígitos verificadores do CNPJ devem ser numéricos.",
  CNPJ_INVALID_CHECK_DIGITS: "Dígitos verificadores do CNPJ não conferem.",
};

export class CnpjError extends AppError {
  public readonly cnpjCode: CnpjErrorCode;

  constructor(cnpjCode: CnpjErrorCode, message?: string, details?: unknown) {
    super(message ?? CNPJ_ERROR_MESSAGES[cnpjCode], cnpjCode, details);
    this.name = "CnpjError";
    this.cnpjCode = cnpjCode;
  }
}

export function cnpjFailure(code: CnpjErrorCode): CnpjFailure {
  return { ok: false, code, message: CNPJ_ERROR_MESSAGES[code] };
}
