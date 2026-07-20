import { describe, expect, it } from "vitest";

import { CnpjError, computeCheckDigits } from "../../core/cnpj/index.js";
import { VALID_VECTORS } from "./vectors.js";

describe("computeCheckDigits", () => {
  it("calcula os DVs do vetor oficial (1º DV = 3, 2º DV = 5)", () => {
    const digits = computeCheckDigits("12ABC34501DE");
    expect(digits).toBe("35");
    expect(digits[0]).toBe("3");
    expect(digits[1]).toBe("5");
  });

  it("reproduz os DVs de todos os vetores válidos a partir das 12 posições", () => {
    for (const vector of VALID_VECTORS) {
      const base = vector.canonical.slice(0, 12);
      expect(computeCheckDigits(base)).toBe(vector.dv);
    }
  });

  it("mantém válido o algoritmo para CNPJ numérico legado", () => {
    expect(computeCheckDigits("112223330001")).toBe("81");
    expect(computeCheckDigits("042520110001")).toBe("10");
  });

  it("rejeita base fora do alfabeto A-Z0-9", () => {
    expect(() => computeCheckDigits("12ABC34501D#")).toThrow(CnpjError);
  });

  it("rejeita base com comprimento diferente de 12", () => {
    expect(() => computeCheckDigits("12ABC34501D")).toThrow(CnpjError);
    expect(() => computeCheckDigits("12ABC34501DEE")).toThrow(CnpjError);
  });

  it("rejeita letras minúsculas na base (sem normalização implícita)", () => {
    expect(() => computeCheckDigits("12abc34501de")).toThrow(CnpjError);
  });
});
