-- Esquema PostgreSQL do CNPJ DB Loader

-- Perfil: full

-- Gerado a partir do modelo interno da Receita Federal.

begin;

-- Tabelas de domínio

create table if not exists countries (
  code text not null,
  description text not null,
  primary key (code)
);

create table if not exists cities (
  code text not null,
  description text not null,
  primary key (code)
);

create table if not exists partner_qualifications (
  code text not null,
  description text not null,
  primary key (code)
);

create table if not exists legal_natures (
  code text not null,
  description text not null,
  primary key (code)
);

create table if not exists cnaes (
  code text not null,
  description text not null,
  primary key (code)
);

create table if not exists reasons (
  code text not null,
  description text not null,
  primary key (code)
);

create table if not exists company_sizes (
  code text not null,
  description text not null,
  primary key (code)
);

create table if not exists branch_types (
  code text not null,
  description text not null,
  primary key (code)
);

create table if not exists registration_statuses (
  code text not null,
  description text not null,
  primary key (code)
);

create table if not exists partner_types (
  code text not null,
  description text not null,
  primary key (code)
);

create table if not exists age_groups (
  code text not null,
  description text not null,
  primary key (code)
);

-- Tabelas operacionais finais (simplificadas para materialização inicial mais rápida)

create table if not exists companies (
  cnpj_root text not null,
  company_name text not null,
  legal_nature_code text not null,
  responsible_qualification_code text not null,
  share_capital numeric(18,2) not null,
  company_size_code text not null,
  responsible_federative_entity text,
  created_at timestamp without time zone not null default now(),
  updated_at timestamp without time zone not null default now(),
  primary key (cnpj_root)
);

create table if not exists establishments (
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
  special_status_date date,
  cnpj_full text not null,
  created_at timestamp without time zone not null default now(),
  updated_at timestamp without time zone not null default now(),
  primary key (cnpj_full)
);

create table if not exists partners (
  id bigserial primary key,
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
  age_group_code text,
  partner_dedupe_key text not null,
  created_at timestamp without time zone not null default now(),
  updated_at timestamp without time zone not null default now(),
  unique (partner_dedupe_key)
);

create table if not exists simples_options (
  cnpj_root text not null,
  simples_option_flag text,
  simples_option_date date,
  simples_exclusion_date date,
  mei_option_flag text,
  mei_option_date date,
  mei_exclusion_date date,
  created_at timestamp without time zone not null default now(),
  updated_at timestamp without time zone not null default now(),
  primary key (cnpj_root),
  constraint chk_simples_flag check (simples_option_flag in ('S', 'N') or simples_option_flag is null or simples_option_flag = ''),
  constraint chk_mei_flag check (mei_option_flag in ('S', 'N') or mei_option_flag is null or mei_option_flag = '')
);

-- Tabelas de controle de importação

create table if not exists import_plans (
  id bigserial primary key,
  source_fingerprint text not null unique,
  input_path text not null,
  validated_path text not null,
  batch_size integer not null,
  target_database text not null,
  total_datasets integer not null,
  total_files integer not null,
  total_rows bigint not null,
  total_batches bigint not null,
  execution_order jsonb not null,
  status text not null default 'planned',
  load_status text not null default 'pending',
  materialization_status text not null default 'pending',
  last_phase text,
  last_error text,
  created_at timestamp with time zone not null default now(),
  updated_at timestamp with time zone not null default now(),
  last_used_at timestamp with time zone not null default now()
);

create table if not exists import_plan_files (
  id bigserial primary key,
  plan_id bigint not null references import_plans (id) on delete cascade,
  dataset text not null,
  dataset_index integer not null,
  file_index integer not null,
  file_path text not null,
  file_display_path text not null,
  file_size bigint not null,
  file_mtime timestamp with time zone not null,
  total_rows bigint not null,
  total_batches bigint not null,
  unique (plan_id, file_path)
);

create table if not exists import_checkpoints (
  id bigserial primary key,
  dataset text not null,
  file_path text not null,
  file_size bigint not null,
  file_mtime timestamp with time zone not null,
  byte_offset bigint not null default 0,
  rows_committed bigint not null default 0,
  status text not null default 'pending',
  last_error text,
  updated_at timestamp with time zone not null default now(),
  unique (dataset, file_path)
);

create table if not exists import_materialization_checkpoints (
  id bigserial primary key,
  plan_id bigint not null references import_plans (id) on delete cascade,
  dataset text not null,
  target_table text not null,
  status text not null default 'pending',
  rows_materialized bigint not null default 0,
  last_staging_id bigint not null default 0,
  chunks_completed bigint not null default 0,
  last_error text,
  started_at timestamp with time zone,
  completed_at timestamp with time zone,
  updated_at timestamp with time zone not null default now(),
  staging_row_count_verified bigint,
  staging_max_staging_id_verified bigint,
  staging_validated_at timestamp with time zone,
  lookup_reconciliation_status text not null default 'pending',
  lookup_reconciliation_row_count_verified bigint,
  lookup_reconciliation_max_staging_id_verified bigint,
  lookup_reconciliation_completed_at timestamp with time zone,
  last_chunk_first_staging_id bigint not null default 0,
  last_chunk_last_staging_id bigint not null default 0,
  last_chunk_rows bigint not null default 0,
  unique (plan_id, dataset)
);

create table if not exists import_quarantine (
  id bigserial primary key,
  dataset text not null,
  file_path text not null,
  row_number bigint,
  checkpoint_offset bigint,
  error_code text,
  error_category text,
  error_stage text,
  error_message text not null,
  raw_line text not null,
  parsed_payload jsonb,
  sanitizations_applied jsonb,
  retry_count integer not null default 0,
  can_retry_later boolean not null default false,
  created_at timestamp with time zone not null default now()
);

-- Tabelas de staging para importações em lote

create unlogged table if not exists staging_companies (
  staging_id bigserial primary key,
  cnpj_root text not null,
  company_name text not null,
  legal_nature_code text not null,
  responsible_qualification_code text not null,
  share_capital numeric(18,2) not null,
  company_size_code text not null,
  responsible_federative_entity text
);

create unlogged table if not exists staging_establishments (
  staging_id bigserial primary key,
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
  staging_id bigserial primary key,
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
  staging_id bigserial primary key,
  cnpj_root text not null,
  simples_option_flag text,
  simples_option_date date,
  simples_exclusion_date date,
  mei_option_flag text,
  mei_option_date date,
  mei_exclusion_date date
);

-- Dados iniciais de domínio

insert into company_sizes (code, description) values
  ('00', 'Not informed'),
  ('01', 'Micro company'),
  ('03', 'Small business'),
  ('05', 'Other')
on conflict (code) do update set description = excluded.description;

insert into branch_types (code, description) values
  ('1', 'Headquarters'),
  ('2', 'Branch')
on conflict (code) do update set description = excluded.description;

insert into registration_statuses (code, description) values
  ('01', 'Null'),
  ('2', 'Active'),
  ('3', 'Suspended'),
  ('4', 'Inactive'),
  ('08', 'Closed')
on conflict (code) do update set description = excluded.description;

insert into partner_types (code, description) values
  ('1', 'Legal entity'),
  ('2', 'Natural person'),
  ('3', 'Foreign person/entity')
on conflict (code) do update set description = excluded.description;

insert into age_groups (code, description) values
  ('0', 'Not applicable'),
  ('1', '0 to 12 years'),
  ('2', '13 to 20 years'),
  ('3', '21 to 30 years'),
  ('4', '31 to 40 years'),
  ('5', '41 to 50 years'),
  ('6', '51 to 60 years'),
  ('7', '61 to 70 years'),
  ('8', '71 to 80 years'),
  ('9', 'Over 80 years')
on conflict (code) do update set description = excluded.description;

commit;
