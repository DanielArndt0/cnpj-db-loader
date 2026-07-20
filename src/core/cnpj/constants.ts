export const CNPJ_LENGTH = 14;
export const CNPJ_BASICO_LENGTH = 8;
export const CNPJ_ORDEM_LENGTH = 4;
export const CNPJ_DV_LENGTH = 2;
export const CNPJ_BASE_LENGTH = CNPJ_BASICO_LENGTH + CNPJ_ORDEM_LENGTH;

export const CNPJ_BASICO_PATTERN = /^[0-9A-Z]{8}$/;
export const CNPJ_ORDEM_PATTERN = /^[0-9A-Z]{4}$/;
export const CNPJ_DV_PATTERN = /^[0-9]{2}$/;
export const CNPJ_BASE_PATTERN = /^[0-9A-Z]{12}$/;
export const CNPJ_CANONICAL_PATTERN = /^[0-9A-Z]{12}[0-9]{2}$/;

export const CNPJ_MASK_PATTERN =
  /^([0-9A-Z]{2})\.([0-9A-Z]{3})\.([0-9A-Z]{3})\/([0-9A-Z]{4})-([0-9]{2})$/;

export const CNPJ_MASK_SEPARATOR = /[./-]/;
export const CNPJ_MASK_SEPARATOR_GLOBAL = /[./-]/g;
export const CNPJ_ASCII_LOWERCASE = /[a-z]/g;

export const CNPJ_FIRST_DV_WEIGHTS = [
  5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2,
] as const;

export const CNPJ_SECOND_DV_WEIGHTS = [
  6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2,
] as const;

export const CNPJ_NORMALIZATION_POLICY = {
  acceptLowercase: true,
  acceptStandardMask: true,
} as const;
