import type { CnpjErrorCode } from "./errors.js";

declare const cnpjCanonicalBrand: unique symbol;

export type CnpjCanonical = string & { readonly [cnpjCanonicalBrand]: true };

export interface CnpjSegments {
  readonly basico: string;
  readonly ordem: string;
  readonly dv: string;
}

export interface CnpjNormalizeOptions {
  readonly acceptLowercase?: boolean;
  readonly acceptStandardMask?: boolean;
}

export interface CnpjSuccess {
  readonly ok: true;
  readonly value: CnpjCanonical;
}

export interface CnpjFailure {
  readonly ok: false;
  readonly code: CnpjErrorCode;
  readonly message: string;
}

export type CnpjResult = CnpjSuccess | CnpjFailure;
