import { describe, expect, it } from "vitest";

import {
  assertCnpj,
  CnpjError,
  isValidCnpj,
  validateCnpj,
} from "../../core/cnpj/index.js";
import { INVALID_VECTORS, VALID_VECTORS } from "./vectors.js";

describe("validateCnpj", () => {
  it("aceita o exemplo oficial e os numéricos legados", () => {
    for (const vector of VALID_VECTORS) {
      expect(isValidCnpj(vector.canonical)).toBe(true);
      expect(isValidCnpj(vector.masked)).toBe(true);

      const result = validateCnpj(vector.masked);
      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.value).toBe(vector.canonical);
      }
    }
  });

  it("aceita entrada em lowercase pela política pública única", () => {
    expect(isValidCnpj("12abc34501de35")).toBe(true);
  });

  it("rejeita quando o DV é adulterado", () => {
    const result = validateCnpj("12ABC34501DE34");
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.code).toBe("CNPJ_INVALID_CHECK_DIGITS");
    }
    expect(isValidCnpj("11222333000182")).toBe(false);
  });

  it("propaga os códigos de erro estruturais e de charset", () => {
    for (const vector of INVALID_VECTORS) {
      const result = validateCnpj(vector.input);
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.code).toBe(vector.code);
      }
    }
  });

  it("assertCnpj retorna a forma canônica quando válido", () => {
    expect(assertCnpj("12.ABC.345/01DE-35")).toBe("12ABC34501DE35");
  });

  it("assertCnpj lança CnpjError com código estável quando inválido", () => {
    expect.assertions(2);
    try {
      assertCnpj("12ABC34501DE34");
    } catch (error) {
      expect(error).toBeInstanceOf(CnpjError);
      if (error instanceof CnpjError) {
        expect(error.cnpjCode).toBe("CNPJ_INVALID_CHECK_DIGITS");
      }
    }
  });
});
