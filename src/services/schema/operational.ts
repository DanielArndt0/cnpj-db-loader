import {
  companiesLayout,
  establishmentsLayout,
  partnersLayout,
  simplesLayout,
} from "../../dictionary/layouts/index.js";
import { createColumnSql } from "./shared.js";
import {
  TABELA_EMPRESAS,
  TABELA_ESTABELECIMENTO_CNAES_SECUNDARIOS,
  TABELA_ESTABELECIMENTOS,
  TABELA_SIMPLES,
  TABELA_SOCIOS,
} from "./table-names.js";

export function createCompaniesSql(): string {
  return [
    `create table if not exists ${TABELA_EMPRESAS} (`,
    companiesLayout.fields.map(createColumnSql).join(",\n") + ",",
    "  criado_em timestamp without time zone not null default now(),",
    "  atualizado_em timestamp without time zone not null default now(),",
    "  primary key (cnpj_basico),",
    "  constraint chk_empresas_cnpj_basico check (cnpj_basico ~ '^[0-9A-Z]{8}$')",
    ");",
  ].join("\n");
}

export function createEstablishmentsSql(): string {
  const baseColumns = establishmentsLayout.fields
    .map(createColumnSql)
    .join(",\n");

  return [
    `create table if not exists ${TABELA_ESTABELECIMENTOS} (`,
    baseColumns + ",",
    "  cnpj_completo text not null,",
    "  criado_em timestamp without time zone not null default now(),",
    "  atualizado_em timestamp without time zone not null default now(),",
    "  primary key (cnpj_completo),",
    "  constraint chk_estabelecimentos_cnpj_basico check (cnpj_basico ~ '^[0-9A-Z]{8}$'),",
    "  constraint chk_estabelecimentos_cnpj_ordem check (cnpj_ordem ~ '^[0-9A-Z]{4}$'),",
    "  constraint chk_estabelecimentos_cnpj_dv check (cnpj_dv ~ '^[0-9]{2}$'),",
    "  constraint chk_estabelecimentos_cnpj_completo check (cnpj_completo ~ '^[0-9A-Z]{12}[0-9]{2}$')",
    ");",
  ].join("\n");
}

export function createPartnersSql(): string {
  return [
    `create table if not exists ${TABELA_SOCIOS} (`,
    "  id bigserial primary key,",
    partnersLayout.fields.map(createColumnSql).join(",\n") + ",",
    "  chave_deduplicacao_socio text not null,",
    "  criado_em timestamp without time zone not null default now(),",
    "  atualizado_em timestamp without time zone not null default now(),",
    "  unique (chave_deduplicacao_socio),",
    "  constraint chk_socios_cnpj_basico check (cnpj_basico ~ '^[0-9A-Z]{8}$')",
    ");",
  ].join("\n");
}

export function createEstablishmentSecondaryCnaesSql(): string {
  return [
    `create table if not exists ${TABELA_ESTABELECIMENTO_CNAES_SECUNDARIOS} (`,
    "  cnpj_completo text not null,",
    "  codigo_cnae text not null,",
    "  primary key (cnpj_completo, codigo_cnae),",
    "  constraint chk_estabelecimento_cnaes_secundarios_cnpj_completo check (cnpj_completo ~ '^[0-9A-Z]{12}[0-9]{2}$')",
    ");",
  ].join("\n");
}

export function createSimplesSql(): string {
  return [
    `create table if not exists ${TABELA_SIMPLES} (`,
    simplesLayout.fields.map(createColumnSql).join(",\n") + ",",
    "  criado_em timestamp without time zone not null default now(),",
    "  atualizado_em timestamp without time zone not null default now(),",
    "  primary key (cnpj_basico),",
    "  constraint chk_simples_cnpj_basico check (cnpj_basico ~ '^[0-9A-Z]{8}$'),",
    "  constraint chk_opcao_simples check (opcao_simples in ('S', 'N') or opcao_simples is null or opcao_simples = ''),",
    "  constraint chk_opcao_mei check (opcao_mei in ('S', 'N') or opcao_mei is null or opcao_mei = '')",
    ");",
  ].join("\n");
}

export function createOperationalSchemaParts(): string[] {
  return [
    "-- Tabelas finais (simplificadas para materialização rápida na primeira carga)",
    createCompaniesSql(),
    createEstablishmentsSql(),
    createEstablishmentSecondaryCnaesSql(),
    createPartnersSql(),
    createSimplesSql(),
  ];
}
