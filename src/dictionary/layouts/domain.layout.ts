import type { TableLayout } from "./types.js";

export const countriesLayout: TableLayout = {
  key: "countries",
  tableName: "countries",
  sourceName: "PAÍSES",
  description: "Country domain table.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "code", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "description", dataType: "text" },
  ],
};

export const citiesLayout: TableLayout = {
  key: "cities",
  tableName: "cities",
  sourceName: "MUNICÍPIOS",
  description: "City domain table.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "code", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "description", dataType: "text" },
  ],
};

export const partnerQualificationsLayout: TableLayout = {
  key: "partner_qualifications",
  tableName: "partner_qualifications",
  sourceName: "QUALIFICAÇÕES DE SÓCIOS",
  description: "Partner qualification domain table.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "code", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "description", dataType: "text" },
  ],
};

export const legalNaturesLayout: TableLayout = {
  key: "legal_natures",
  tableName: "legal_natures",
  sourceName: "NATUREZAS JURÍDICAS",
  description: "Legal nature domain table.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "code", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "description", dataType: "text" },
  ],
};

export const cnaesLayout: TableLayout = {
  key: "cnaes",
  tableName: "cnaes",
  sourceName: "CNAEs",
  description: "Economic activity domain table.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "code", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "description", dataType: "text" },
  ],
};

export const reasonsLayout: TableLayout = {
  key: "reasons",
  tableName: "reasons",
  sourceName: "MOTIVOS",
  description: "Registration status reason domain table.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "code", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "description", dataType: "text" },
  ],
};

export const companySizesLayout: TableLayout = {
  key: "company_sizes",
  tableName: "company_sizes",
  sourceName: "INTERNAL COMPANY SIZE LOOKUP",
  description:
    "Internal lookup table for company size codes defined by the Receita layout.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "code", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "description", dataType: "text" },
  ],
};

export const branchTypesLayout: TableLayout = {
  key: "branch_types",
  tableName: "branch_types",
  sourceName: "INTERNAL BRANCH TYPE LOOKUP",
  description:
    "Internal lookup table for matriz/filial codes defined by the Receita layout.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "code", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "description", dataType: "text" },
  ],
};

export const registrationStatusesLayout: TableLayout = {
  key: "registration_statuses",
  tableName: "registration_statuses",
  sourceName: "INTERNAL REGISTRATION STATUS LOOKUP",
  description:
    "Internal lookup table for establishment registration status codes defined by the Receita layout.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "code", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "description", dataType: "text" },
  ],
};

export const partnerTypesLayout: TableLayout = {
  key: "partner_types",
  tableName: "partner_types",
  sourceName: "INTERNAL PARTNER TYPE LOOKUP",
  description:
    "Internal lookup table for partner identifier codes defined by the Receita layout.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "code", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "description", dataType: "text" },
  ],
};

export const ageGroupsLayout: TableLayout = {
  key: "age_groups",
  tableName: "age_groups",
  sourceName: "INTERNAL AGE GROUP LOOKUP",
  description:
    "Internal lookup table for partner age group codes defined by the Receita layout.",
  fields: [
    { sourceLabel: "CÓDIGO", columnName: "code", dataType: "text" },
    { sourceLabel: "DESCRIÇÃO", columnName: "description", dataType: "text" },
  ],
};
