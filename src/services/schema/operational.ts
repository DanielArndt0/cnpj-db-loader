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
    "  primary key (cnpj_root),",
    "  constraint fk_companies_legal_nature foreign key (legal_nature_code) references legal_natures (code),",
    "  constraint fk_companies_responsible_qualification foreign key (responsible_qualification_code) references partner_qualifications (code),",
    "  constraint fk_companies_company_size foreign key (company_size_code) references company_sizes (code)",
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
    "  cnpj_full text generated always as (cnpj_root || cnpj_order || cnpj_check_digits) stored,",
    "  created_at timestamp without time zone not null default now(),",
    "  updated_at timestamp without time zone not null default now(),",
    "  primary key (cnpj_full),",
    "  unique (cnpj_root, cnpj_order, cnpj_check_digits),",
    "  constraint fk_establishments_company foreign key (cnpj_root) references companies (cnpj_root),",
    "  constraint fk_establishments_branch_type foreign key (branch_type_code) references branch_types (code),",
    "  constraint fk_establishments_registration_status foreign key (registration_status_code) references registration_statuses (code),",
    "  constraint fk_establishments_registration_reason foreign key (registration_status_reason_code) references reasons (code),",
    "  constraint fk_establishments_country foreign key (country_code) references countries (code),",
    "  constraint fk_establishments_main_cnae foreign key (main_cnae_code) references cnaes (code),",
    "  constraint fk_establishments_city foreign key (city_code) references cities (code)",
    ");",
  ].join("\n");
}

export function createEstablishmentSecondaryCnaesSql(): string {
  return [
    "create table if not exists establishment_secondary_cnaes (",
    "  establishment_cnpj_full text not null,",
    "  cnae_code text not null,",
    "  source_order integer not null,",
    "  created_at timestamp without time zone not null default now(),",
    "  primary key (establishment_cnpj_full, cnae_code),",
    "  constraint fk_establishment_secondary_cnaes_establishment foreign key (establishment_cnpj_full) references establishments (cnpj_full) on delete cascade,",
    "  constraint fk_establishment_secondary_cnaes_cnae foreign key (cnae_code) references cnaes (code)",
    ");",
  ].join("\n");
}

export function createPartnersSql(): string {
  return [
    "create table if not exists partners (",
    "  id bigserial primary key,",
    partnersLayout.fields.map(createColumnSql).join(",\n") + ",",
    "  partner_dedupe_key text generated always as (md5(concat_ws('|',",
    "    coalesce(cnpj_root, ''),",
    "    coalesce(partner_type_code, ''),",
    "    coalesce(partner_name, ''),",
    "    coalesce(partner_document, ''),",
    "    coalesce(partner_qualification_code, ''),",
    "    coalesce(entry_date::text, ''),",
    "    coalesce(country_code, ''),",
    "    coalesce(legal_representative_document, ''),",
    "    coalesce(legal_representative_name, ''),",
    "    coalesce(legal_representative_qualification_code, ''),",
    "    coalesce(age_group_code, '')",
    "  ))) stored,",
    "  created_at timestamp without time zone not null default now(),",
    "  updated_at timestamp without time zone not null default now(),",
    "  unique (partner_dedupe_key),",
    "  constraint fk_partners_company foreign key (cnpj_root) references companies (cnpj_root),",
    "  constraint fk_partners_partner_type foreign key (partner_type_code) references partner_types (code),",
    "  constraint fk_partners_partner_qualification foreign key (partner_qualification_code) references partner_qualifications (code),",
    "  constraint fk_partners_country foreign key (country_code) references countries (code),",
    "  constraint fk_partners_legal_representative_qualification foreign key (legal_representative_qualification_code) references partner_qualifications (code),",
    "  constraint fk_partners_age_group foreign key (age_group_code) references age_groups (code)",
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
    "  constraint fk_simples_company foreign key (cnpj_root) references companies (cnpj_root),",
    "  constraint chk_simples_flag check (simples_option_flag in ('S', 'N') or simples_option_flag is null or simples_option_flag = ''),",
    "  constraint chk_mei_flag check (mei_option_flag in ('S', 'N') or mei_option_flag is null or mei_option_flag = '')",
    ");",
  ].join("\n");
}

export function createOperationalSchemaParts(): string[] {
  return [
    "-- Final relational tables",
    createCompaniesSql(),
    createEstablishmentsSql(),
    createEstablishmentSecondaryCnaesSql(),
    createPartnersSql(),
    createSimplesSql(),
  ];
}
