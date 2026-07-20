import { describe, expect, it } from "vitest";

import {
  composeCnpjCompleto,
  CnpjError,
  splitCnpj,
} from "../../core/cnpj/index.js";
import { VALID_VECTORS } from "./vectors.js";

describe("composeCnpjCompleto / splitCnpj", () => {
  it("compõe e decompõe preservando 8 + 4 + 2 em todos os vetores", () => {
    for (const vector of VALID_VECTORS) {
      const composed = composeCnpjCompleto(
        vector.basico,
        vector.ordem,
        vector.dv,
      );
      expect(composed).toBe(vector.canonical);

      const segments = splitCnpj(vector.canonical);
      expect(segments.basico).toBe(vector.basico);
      expect(segments.ordem).toBe(vector.ordem);
      expect(segments.dv).toBe(vector.dv);
    }
  });

  it("preserva zeros à esquerda no básico e na ordem sem cast numérico", () => {
    const composed = composeCnpjCompleto("00000000", "0001", "00");
    expect(composed).toBe("00000000000100");

    const segments = splitCnpj("00000000000100");
    expect(segments.basico).toBe("00000000");
    expect(segments.ordem).toBe("0001");
    expect(segments.dv).toBe("00");
  });

  it("é reversível: compose(split(x)) === x", () => {
    for (const vector of VALID_VECTORS) {
      const segments = splitCnpj(vector.canonical);
      const recomposed = composeCnpjCompleto(
        segments.basico,
        segments.ordem,
        segments.dv,
      );
      expect(recomposed).toBe(vector.canonical);
    }
  });

  it("rejeita segmentos com tamanho ou alfabeto inválidos", () => {
    expect(() => composeCnpjCompleto("12ABC34", "01DE", "35")).toThrow(
      CnpjError,
    );
    expect(() => composeCnpjCompleto("12ABC345", "01D", "35")).toThrow(
      CnpjError,
    );
    expect(() => composeCnpjCompleto("12ABC345", "01DE", "3A")).toThrow(
      CnpjError,
    );
  });

  it("rejeita split de entrada não canônica", () => {
    expect(() => splitCnpj("12.ABC.345/01DE-35")).toThrow(CnpjError);
    expect(() => splitCnpj("12ABC34501DE3A")).toThrow(CnpjError);
  });
});
