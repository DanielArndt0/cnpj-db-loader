import type { TableLayout } from "./types.js";

export const simplesLayout: TableLayout = {
  key: "simples_options",
  tableName: "simples_options",
  sourceName: "DADOS DO SIMPLES",
  description: "Simples Nacional and MEI option block.",
  fields: [
    { sourceLabel: "CNPJ BÁSICO", columnName: "cnpj_root", dataType: "text" },
    {
      sourceLabel: "OPÇÃO PELO SIMPLES",
      columnName: "simples_option_flag",
      dataType: "text",
      nullable: true,
    },
    {
      sourceLabel: "DATA DE OPÇÃO PELO SIMPLES",
      columnName: "simples_option_date",
      dataType: "date",
      nullable: true,
    },
    {
      sourceLabel: "DATA DE EXCLUSÃO DO SIMPLES",
      columnName: "simples_exclusion_date",
      dataType: "date",
      nullable: true,
    },
    {
      sourceLabel: "OPÇÃO PELO MEI",
      columnName: "mei_option_flag",
      dataType: "text",
      nullable: true,
    },
    {
      sourceLabel: "DATA DE OPÇÃO PELO MEI",
      columnName: "mei_option_date",
      dataType: "date",
      nullable: true,
    },
    {
      sourceLabel: "DATA DE EXCLUSÃO DO MEI",
      columnName: "mei_exclusion_date",
      dataType: "date",
      nullable: true,
    },
  ],
};
