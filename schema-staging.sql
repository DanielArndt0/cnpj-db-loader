-- CNPJ DB Loader PostgreSQL schema

-- Profile: staging

-- Generated from the internal Receita Federal model.

begin;

-- Staging tables for bulk-oriented imports

create unlogged table if not exists staging_companies (
  cnpj_root text not null,
  company_name text not null,
  legal_nature_code text not null,
  responsible_qualification_code text not null,
  share_capital numeric(18,2) not null,
  company_size_code text not null,
  responsible_federative_entity text
);

create unlogged table if not exists staging_establishments (
  cnpj_root text not null,
  cnpj_order text not null,
  cnpj_check_digits text not null,
  branch_type_code text not null,
  trade_name text,
  registration_status_code text not null,
  registration_status_date date,
  registration_status_reason_code text,
  foreign_city_name text,
  country_code text,
  activity_start_date date,
  main_cnae_code text not null,
  secondary_cnaes_raw text,
  street_type text,
  street_name text,
  street_number text,
  address_complement text,
  district text,
  postal_code text,
  state_code text,
  city_code text,
  phone_area_code_1 text,
  phone_number_1 text,
  phone_area_code_2 text,
  phone_number_2 text,
  fax_area_code text,
  fax_number text,
  email text,
  special_status text,
  special_status_date date
);

create unlogged table if not exists staging_partners (
  cnpj_root text not null,
  partner_type_code text not null,
  partner_name text not null,
  partner_document text,
  partner_qualification_code text not null,
  entry_date date,
  country_code text,
  legal_representative_document text,
  legal_representative_name text,
  legal_representative_qualification_code text,
  age_group_code text
);

create unlogged table if not exists staging_simples_options (
  cnpj_root text not null,
  simples_option_flag text,
  simples_option_date date,
  simples_exclusion_date date,
  mei_option_flag text,
  mei_option_date date,
  mei_exclusion_date date
);

create unlogged table if not exists staging_establishment_secondary_cnaes (
  establishment_cnpj_full text not null,
  cnae_code text not null,
  source_order integer not null
);

commit;
