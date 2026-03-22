import type { TableLayout } from "./types.js";

export const partnersLayout: TableLayout = {
  key: "partners",
  tableName: "partners",
  sourceName: "SÓCIOS",
  description:
    "Partners block, including masked CPF/CNPJ fields according to the official layout.",
  fields: [
    { sourceLabel: "CNPJ BÁSICO", columnName: "cnpj_root", dataType: "text" },
    {
      sourceLabel: "IDENTIFICADOR DE SÓCIO",
      columnName: "partner_type_code",
      dataType: "text",
    },
    {
      sourceLabel: "NOME DO SÓCIO / RAZÃO SOCIAL",
      columnName: "partner_name",
      dataType: "text",
    },
    {
      sourceLabel: "CNPJ/CPF DO SÓCIO",
      columnName: "partner_document",
      dataType: "text",
      nullable: true,
    },
    {
      sourceLabel: "QUALIFICAÇÃO DO SÓCIO",
      columnName: "partner_qualification_code",
      dataType: "text",
    },
    {
      sourceLabel: "DATA DE ENTRADA SOCIEDADE",
      columnName: "entry_date",
      dataType: "date",
      nullable: true,
    },
    {
      sourceLabel: "PAIS",
      columnName: "country_code",
      dataType: "text",
      nullable: true,
    },
    {
      sourceLabel: "REPRESENTANTE LEGAL",
      columnName: "legal_representative_document",
      dataType: "text",
      nullable: true,
    },
    {
      sourceLabel: "NOME DO REPRESENTANTE",
      columnName: "legal_representative_name",
      dataType: "text",
      nullable: true,
    },
    {
      sourceLabel: "QUALIFICAÇÃO DO REPRESENTANTE LEGAL",
      columnName: "legal_representative_qualification_code",
      dataType: "text",
      nullable: true,
    },
    {
      sourceLabel: "FAIXA ETÁRIA",
      columnName: "age_group_code",
      dataType: "text",
      nullable: true,
    },
  ],
};
