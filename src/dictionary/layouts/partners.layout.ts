import type { TableLayout } from "./types.js";

export const partnersLayout: TableLayout = {
  key: "partners",
  tableName: "socios",
  sourceName: "SÓCIOS",
  description:
    "Partners block, including masked CPF/CNPJ fields according to the official layout.",
  fields: [
    { sourceLabel: "CNPJ BÁSICO", columnName: "cnpj_basico", dataType: "text" },
    {
      sourceLabel: "IDENTIFICADOR DE SÓCIO",
      columnName: "identificador_socio",
      dataType: "text",
    },
    {
      sourceLabel: "NOME DO SÓCIO / RAZÃO SOCIAL",
      columnName: "nome_socio_razao_social",
      dataType: "text",
    },
    {
      sourceLabel: "CNPJ/CPF DO SÓCIO",
      columnName: "cnpj_cpf_socio",
      dataType: "text",
      nullable: true,
    },
    {
      sourceLabel: "QUALIFICAÇÃO DO SÓCIO",
      columnName: "codigo_qualificacao_socio",
      dataType: "text",
    },
    {
      sourceLabel: "DATA DE ENTRADA SOCIEDADE",
      columnName: "data_entrada_sociedade",
      dataType: "date",
      nullable: true,
    },
    {
      sourceLabel: "PAIS",
      columnName: "codigo_pais",
      dataType: "text",
      nullable: true,
    },
    {
      sourceLabel: "REPRESENTANTE LEGAL",
      columnName: "cpf_representante_legal",
      dataType: "text",
      nullable: true,
    },
    {
      sourceLabel: "NOME DO REPRESENTANTE",
      columnName: "nome_representante_legal",
      dataType: "text",
      nullable: true,
    },
    {
      sourceLabel: "QUALIFICAÇÃO DO REPRESENTANTE LEGAL",
      columnName: "codigo_qualificacao_representante_legal",
      dataType: "text",
      nullable: true,
    },
    {
      sourceLabel: "FAIXA ETÁRIA",
      columnName: "codigo_faixa_etaria",
      dataType: "text",
      nullable: true,
    },
  ],
};
