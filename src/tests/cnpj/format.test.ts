import { describe, expect, it } from "vitest";

import { CnpjError, formatCnpj, stripCnpjMask } from "../../core/cnpj/index.js";
import { VALID_VECTORS } from "./vectors.js";

describe("formatCnpj / stripCnpjMask", () => {
  it("formata a forma canônica na máscara de exibição", () => {
    for (const vector of VALID_VECTORS) {
      expect(formatCnpj(vector.canonical)).toBe(vector.masked);
    }
  });

  it("remove a máscara retornando a forma canônica", () => {
    for (const vector of VALID_VECTORS) {
      expect(stripCnpjMask(vector.masked)).toBe(vector.canonical);
    }
  });

  it("é reversível nos dois sentidos (round-trip)", () => {
    for (const vector of VALID_VECTORS) {
      expect(stripCnpjMask(formatCnpj(vector.canonical))).toBe(
        vector.canonical,
      );
      expect(formatCnpj(stripCnpjMask(vector.masked))).toBe(vector.masked);
    }
  });

  it("stripCnpjMask é idempotente sobre a forma canônica", () => {
    expect(stripCnpjMask("12ABC34501DE35")).toBe("12ABC34501DE35");
  });

  it("stripCnpjMask rejeita máscara incompleta em vez de apagar separadores", () => {
    expect(() => stripCnpjMask("12ABC34501DE-35")).toThrow(CnpjError);
  });

  it("formatCnpj rejeita entrada não canônica", () => {
    expect(() => formatCnpj("12ABC34501DE3")).toThrow(CnpjError);
    expect(() => formatCnpj("12ABC34501DE3A")).toThrow(CnpjError);
  });
});
