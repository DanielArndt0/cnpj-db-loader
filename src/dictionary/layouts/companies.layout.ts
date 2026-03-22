import type { TableLayout } from "./types.js";

export const companiesLayout: TableLayout = {
  key: "companies",
  tableName: "companies",
  sourceName: "EMPRESAS",
  description: "Main company registration block.",
  fields: [
    { sourceLabel: "CNPJ BÁSICO", columnName: "cnpj_root", dataType: "text" },
    {
      sourceLabel: "RAZÃO SOCIAL / NOME EMPRESARIAL",
      columnName: "company_name",
      dataType: "text",
    },
    {
      sourceLabel: "NATUREZA JURÍDICA",
      columnName: "legal_nature_code",
      dataType: "text",
    },
    {
      sourceLabel: "QUALIFICAÇÃO DO RESPONSÁVEL",
      columnName: "responsible_qualification_code",
      dataType: "text",
    },
    {
      sourceLabel: "CAPITAL SOCIAL DA EMPRESA",
      columnName: "share_capital",
      dataType: "numeric",
    },
    {
      sourceLabel: "PORTE DA EMPRESA",
      columnName: "company_size_code",
      dataType: "text",
    },
    {
      sourceLabel: "ENTE FEDERATIVO RESPONSÁVEL",
      columnName: "responsible_federative_entity",
      dataType: "text",
      nullable: true,
    },
  ],
};
