import type { TableLayout } from "./types.js";

export const simplesLayout: TableLayout = {
  key: "simples_options",
  tableName: "simples",
  sourceName: "DADOS DO SIMPLES",
  description: "Simples Nacional and MEI option block.",
  fields: [
    { sourceLabel: "CNPJ BÁSICO", columnName: "cnpj_basico", dataType: "text" },
    {
      sourceLabel: "OPÇÃO PELO SIMPLES",
      columnName: "opcao_simples",
      dataType: "text",
      nullable: true,
    },
    {
      sourceLabel: "DATA DE OPÇÃO PELO SIMPLES",
      columnName: "data_opcao_simples",
      dataType: "date",
      nullable: true,
    },
    {
      sourceLabel: "DATA DE EXCLUSÃO DO SIMPLES",
      columnName: "data_exclusao_simples",
      dataType: "date",
      nullable: true,
    },
    {
      sourceLabel: "OPÇÃO PELO MEI",
      columnName: "opcao_mei",
      dataType: "text",
      nullable: true,
    },
    {
      sourceLabel: "DATA DE OPÇÃO PELO MEI",
      columnName: "data_opcao_mei",
      dataType: "date",
      nullable: true,
    },
    {
      sourceLabel: "DATA DE EXCLUSÃO DO MEI",
      columnName: "data_exclusao_mei",
      dataType: "date",
      nullable: true,
    },
  ],
};
