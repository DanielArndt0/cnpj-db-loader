import type { TableLayout } from "./types.js";

export const countriesLayout: TableLayout = {
  key: "countries",
  tableName: "paises",
  sourceName: "PAÍSES",
  description: "Country domain table.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "codigo", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "descricao", dataType: "text" },
  ],
};

export const citiesLayout: TableLayout = {
  key: "cities",
  tableName: "municipios",
  sourceName: "MUNICÍPIOS",
  description: "City domain table.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "codigo", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "descricao", dataType: "text" },
  ],
};

export const partnerQualificationsLayout: TableLayout = {
  key: "partner_qualifications",
  tableName: "qualificacoes_socios",
  sourceName: "QUALIFICAÇÕES DE SÓCIOS",
  description: "Partner qualification domain table.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "codigo", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "descricao", dataType: "text" },
  ],
};

export const legalNaturesLayout: TableLayout = {
  key: "legal_natures",
  tableName: "naturezas_juridicas",
  sourceName: "NATUREZAS JURÍDICAS",
  description: "Legal nature domain table.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "codigo", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "descricao", dataType: "text" },
  ],
};

export const cnaesLayout: TableLayout = {
  key: "cnaes",
  tableName: "cnaes",
  sourceName: "CNAEs",
  description: "Economic activity domain table.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "codigo", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "descricao", dataType: "text" },
  ],
};

export const reasonsLayout: TableLayout = {
  key: "reasons",
  tableName: "motivos_situacao_cadastral",
  sourceName: "MOTIVOS",
  description: "Registration status reason domain table.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "codigo", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "descricao", dataType: "text" },
  ],
};

export const companySizesLayout: TableLayout = {
  key: "company_sizes",
  tableName: "portes_empresa",
  sourceName: "INTERNAL COMPANY SIZE LOOKUP",
  description:
    "Internal lookup table for company size codes defined by the Receita layout.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "codigo", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "descricao", dataType: "text" },
  ],
};

export const branchTypesLayout: TableLayout = {
  key: "branch_types",
  tableName: "identificadores_matriz_filial",
  sourceName: "INTERNAL BRANCH TYPE LOOKUP",
  description:
    "Internal lookup table for matriz/filial codes defined by the Receita layout.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "codigo", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "descricao", dataType: "text" },
  ],
};

export const registrationStatusesLayout: TableLayout = {
  key: "registration_statuses",
  tableName: "situacoes_cadastrais",
  sourceName: "INTERNAL REGISTRATION STATUS LOOKUP",
  description:
    "Internal lookup table for establishment registration status codes defined by the Receita layout.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "codigo", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "descricao", dataType: "text" },
  ],
};

export const partnerTypesLayout: TableLayout = {
  key: "partner_types",
  tableName: "identificadores_socio",
  sourceName: "INTERNAL PARTNER TYPE LOOKUP",
  description:
    "Internal lookup table for partner identifier codes defined by the Receita layout.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "codigo", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "descricao", dataType: "text" },
  ],
};

export const ageGroupsLayout: TableLayout = {
  key: "age_groups",
  tableName: "faixas_etarias",
  sourceName: "INTERNAL AGE GROUP LOOKUP",
  description:
    "Internal lookup table for partner age group codes defined by the Receita layout.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "codigo", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "descricao", dataType: "text" },
  ],
};
