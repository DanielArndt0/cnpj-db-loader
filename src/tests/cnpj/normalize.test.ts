import { describe, expect, it } from "vitest";

import { normalizeCnpj } from "../../core/cnpj/index.js";
import { INVALID_VECTORS, VALID_VECTORS } from "./vectors.js";

describe("normalizeCnpj", () => {
  it("normaliza a máscara e a forma canônica para o mesmo resultado", () => {
    for (const vector of VALID_VECTORS) {
      const fromMasked = normalizeCnpj(vector.masked);
      const fromCanonical = normalizeCnpj(vector.canonical);

      expect(fromMasked.ok).toBe(true);
      expect(fromCanonical.ok).toBe(true);
      if (fromMasked.ok && fromCanonical.ok) {
        expect(fromMasked.value).toBe(vector.canonical);
        expect(fromCanonical.value).toBe(vector.canonical);
      }
    }
  });

  it("remove espaços apenas nas extremidades", () => {
    const result = normalizeCnpj("  12ABC34501DE35  ");
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value).toBe("12ABC34501DE35");
    }
  });

  it("aplica a política pública de lowercase (a-z -> A-Z)", () => {
    const result = normalizeCnpj("12abc34501de35");
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value).toBe("12ABC34501DE35");
    }
  });

  it("rejeita lowercase quando a política é desligada explicitamente", () => {
    const result = normalizeCnpj("12abc34501de35", { acceptLowercase: false });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.code).toBe("CNPJ_INVALID_CHARSET");
    }
  });

  it("rejeita máscara quando a política de máscara é desligada", () => {
    const result = normalizeCnpj("12.ABC.345/01DE-35", {
      acceptStandardMask: false,
    });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.code).toBe("CNPJ_INVALID_MASK");
    }
  });

  it("preserva zeros à esquerda no básico e na ordem", () => {
    const result = normalizeCnpj("00000000000100");
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value.slice(0, 8)).toBe("00000000");
      expect(result.value.slice(8, 12)).toBe("0001");
    }
  });

  it("não apaga caracteres inválidos silenciosamente", () => {
    for (const vector of INVALID_VECTORS) {
      const result = normalizeCnpj(vector.input);
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.code).toBe(vector.code);
      }
    }
  });
});
