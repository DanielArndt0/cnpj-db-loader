import type { CnpjErrorCode } from "../../core/cnpj/index.js";

export interface ValidVector {
  readonly description: string;
  readonly canonical: string;
  readonly masked: string;
  readonly basico: string;
  readonly ordem: string;
  readonly dv: string;
}

export interface InvalidVector {
  readonly description: string;
  readonly input: string | null | undefined;
  readonly code: CnpjErrorCode;
}

export const VALID_VECTORS: readonly ValidVector[] = [
  {
    description: "exemplo oficial alfanumérico",
    canonical: "12ABC34501DE35",
    masked: "12.ABC.345/01DE-35",
    basico: "12ABC345",
    ordem: "01DE",
    dv: "35",
  },
  {
    description: "numérico legado",
    canonical: "11222333000181",
    masked: "11.222.333/0001-81",
    basico: "11222333",
    ordem: "0001",
    dv: "81",
  },
  {
    description: "numérico legado com zero inicial",
    canonical: "04252011000110",
    masked: "04.252.011/0001-10",
    basico: "04252011",
    ordem: "0001",
    dv: "10",
  },
];

export const INVALID_VECTORS: readonly InvalidVector[] = [
  {
    description: "string vazia",
    input: "",
    code: "CNPJ_EMPTY",
  },
  {
    description: "somente espaços",
    input: "   ",
    code: "CNPJ_EMPTY",
  },
  {
    description: "nulo",
    input: null,
    code: "CNPJ_EMPTY",
  },
  {
    description: "indefinido",
    input: undefined,
    code: "CNPJ_EMPTY",
  },
  {
    description: "13 caracteres",
    input: "1122233300018",
    code: "CNPJ_INVALID_LENGTH",
  },
  {
    description: "15 caracteres",
    input: "112223330001811",
    code: "CNPJ_INVALID_LENGTH",
  },
  {
    description: "símbolo nas 12 primeiras posições",
    input: "12ABC3450#DE35",
    code: "CNPJ_INVALID_CHARSET",
  },
  {
    description: "caractere acentuado",
    input: "12ÁBC34501DE35",
    code: "CNPJ_INVALID_CHARSET",
  },
  {
    description: "whitespace interno",
    input: "12ABC3 501DE35",
    code: "CNPJ_INVALID_CHARSET",
  },
  {
    description: "letra em posição de DV",
    input: "12ABC34501DE3A",
    code: "CNPJ_INVALID_DV_CHARSET",
  },
  {
    description: "máscara incompleta",
    input: "12ABC34501DE-35",
    code: "CNPJ_INVALID_MASK",
  },
];
