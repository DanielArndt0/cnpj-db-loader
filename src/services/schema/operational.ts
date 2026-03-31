import {
  companiesLayout,
  establishmentsLayout,
  partnersLayout,
  simplesLayout,
} from "../../dictionary/layouts/index.js";
import { createColumnSql } from "./shared.js";

export function createCompaniesSql(): string {
  return [
    "create table if not exists companies (",
    companiesLayout.fields.map(createColumnSql).join(",\n") + ",",
    "  created_at timestamp without time zone not null default now(),",
    "  updated_at timestamp without time zone not null default now(),",
    "  primary key (cnpj_root)",
    ");",
  ].join("\n");
}

export function createEstablishmentsSql(): string {
  const baseColumns = establishmentsLayout.fields
    .map(createColumnSql)
    .join(",\n");

  return [
    "create table if not exists establishments (",
    baseColumns + ",",
    "  cnpj_full text not null,",
    "  created_at timestamp without time zone not null default now(),",
    "  updated_at timestamp without time zone not null default now(),",
    "  primary key (cnpj_full)",
    ");",
  ].join("\n");
}

export function createPartnersSql(): string {
  return [
    "create table if not exists partners (",
    "  id bigserial primary key,",
    partnersLayout.fields.map(createColumnSql).join(",\n") + ",",
    "  partner_dedupe_key text not null,",
    "  created_at timestamp without time zone not null default now(),",
    "  updated_at timestamp without time zone not null default now(),",
    "  unique (partner_dedupe_key)",
    ");",
  ].join("\n");
}

export function createSimplesSql(): string {
  return [
    "create table if not exists simples_options (",
    simplesLayout.fields.map(createColumnSql).join(",\n") + ",",
    "  created_at timestamp without time zone not null default now(),",
    "  updated_at timestamp without time zone not null default now(),",
    "  primary key (cnpj_root),",
    "  constraint chk_simples_flag check (simples_option_flag in ('S', 'N') or simples_option_flag is null or simples_option_flag = ''),",
    "  constraint chk_mei_flag check (mei_option_flag in ('S', 'N') or mei_option_flag is null or mei_option_flag = '')",
    ");",
  ].join("\n");
}

export function createOperationalSchemaParts(): string[] {
  return [
    "-- Final operational tables (simplified for fast first-load materialization)",
    createCompaniesSql(),
    createEstablishmentsSql(),
    createPartnersSql(),
    createSimplesSql(),
  ];
}
