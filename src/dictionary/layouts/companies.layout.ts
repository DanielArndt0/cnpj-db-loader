import type { TableLayout } from "./types.js";

export const companiesLayout: TableLayout = {
  key: "companies",
  tableName: "empresas",
  sourceName: "EMPRESAS",
  description: "Main company registration block.",
  fields: [
    { sourceLabel: "CNPJ BÁSICO", columnName: "cnpj_basico", dataType: "text" },
    {
      sourceLabel: "RAZÃO SOCIAL / NOME EMPRESARIAL",
      columnName: "razao_social_nome_empresarial",
      dataType: "text",
    },
    {
      sourceLabel: "NATUREZA JURÍDICA",
      columnName: "codigo_natureza_juridica",
      dataType: "text",
    },
    {
      sourceLabel: "QUALIFICAÇÃO DO RESPONSÁVEL",
      columnName: "codigo_qualificacao_responsavel",
      dataType: "text",
    },
    {
      sourceLabel: "CAPITAL SOCIAL DA EMPRESA",
      columnName: "capital_social",
      dataType: "numeric",
    },
    {
      sourceLabel: "PORTE DA EMPRESA",
      columnName: "codigo_porte_empresa",
      dataType: "text",
    },
    {
      sourceLabel: "ENTE FEDERATIVO RESPONSÁVEL",
      columnName: "ente_federativo_responsavel",
      dataType: "text",
      nullable: true,
    },
  ],
};
