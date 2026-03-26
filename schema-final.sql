-- CNPJ DB Loader PostgreSQL schema

-- Profile: final

-- Generated from the internal Receita Federal model.

begin;

-- Domain tables

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

-- Final relational tables

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
  primary key (cnpj_root),
  constraint fk_companies_legal_nature foreign key (legal_nature_code) references legal_natures (code),
  constraint fk_companies_responsible_qualification foreign key (responsible_qualification_code) references partner_qualifications (code),
  constraint fk_companies_company_size foreign key (company_size_code) references company_sizes (code)
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
  cnpj_full text generated always as (cnpj_root || cnpj_order || cnpj_check_digits) stored,
  created_at timestamp without time zone not null default now(),
  updated_at timestamp without time zone not null default now(),
  primary key (cnpj_full),
  unique (cnpj_root, cnpj_order, cnpj_check_digits),
  constraint fk_establishments_company foreign key (cnpj_root) references companies (cnpj_root),
  constraint fk_establishments_branch_type foreign key (branch_type_code) references branch_types (code),
  constraint fk_establishments_registration_status foreign key (registration_status_code) references registration_statuses (code),
  constraint fk_establishments_registration_reason foreign key (registration_status_reason_code) references reasons (code),
  constraint fk_establishments_country foreign key (country_code) references countries (code),
  constraint fk_establishments_main_cnae foreign key (main_cnae_code) references cnaes (code),
  constraint fk_establishments_city foreign key (city_code) references cities (code)
);

create table if not exists establishment_secondary_cnaes (
  establishment_cnpj_full text not null,
  cnae_code text not null,
  source_order integer not null,
  created_at timestamp without time zone not null default now(),
  primary key (establishment_cnpj_full, cnae_code),
  constraint fk_establishment_secondary_cnaes_establishment foreign key (establishment_cnpj_full) references establishments (cnpj_full) on delete cascade,
  constraint fk_establishment_secondary_cnaes_cnae foreign key (cnae_code) references cnaes (code)
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
  partner_dedupe_key text generated always as (md5(concat_ws('|',
    coalesce(cnpj_root, ''),
    coalesce(partner_type_code, ''),
    coalesce(partner_name, ''),
    coalesce(partner_document, ''),
    coalesce(partner_qualification_code, ''),
    coalesce(entry_date::text, ''),
    coalesce(country_code, ''),
    coalesce(legal_representative_document, ''),
    coalesce(legal_representative_name, ''),
    coalesce(legal_representative_qualification_code, ''),
    coalesce(age_group_code, '')
  ))) stored,
  created_at timestamp without time zone not null default now(),
  updated_at timestamp without time zone not null default now(),
  unique (partner_dedupe_key),
  constraint fk_partners_company foreign key (cnpj_root) references companies (cnpj_root),
  constraint fk_partners_partner_type foreign key (partner_type_code) references partner_types (code),
  constraint fk_partners_partner_qualification foreign key (partner_qualification_code) references partner_qualifications (code),
  constraint fk_partners_country foreign key (country_code) references countries (code),
  constraint fk_partners_legal_representative_qualification foreign key (legal_representative_qualification_code) references partner_qualifications (code),
  constraint fk_partners_age_group foreign key (age_group_code) references age_groups (code)
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
  constraint fk_simples_company foreign key (cnpj_root) references companies (cnpj_root),
  constraint chk_simples_flag check (simples_option_flag in ('S', 'N') or simples_option_flag is null or simples_option_flag = ''),
  constraint chk_mei_flag check (mei_option_flag in ('S', 'N') or mei_option_flag is null or mei_option_flag = '')
);

-- Import control tables

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

-- Domain seed data

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

-- Operational indexes
create index if not exists idx_companies_company_name on companies (company_name);
create index if not exists idx_companies_legal_nature_code on companies (legal_nature_code);
create index if not exists idx_establishments_cnpj_root on establishments (cnpj_root);
create index if not exists idx_establishments_city_code on establishments (city_code);
create index if not exists idx_establishments_state_code on establishments (state_code);
create index if not exists idx_establishments_main_cnae_code on establishments (main_cnae_code);
create index if not exists idx_establishments_registration_status_code on establishments (registration_status_code);
create index if not exists idx_partners_cnpj_root on partners (cnpj_root);
create index if not exists idx_partners_partner_document on partners (partner_document);
create index if not exists idx_partners_partner_name on partners (partner_name);
create index if not exists idx_partners_dedupe_key on partners (partner_dedupe_key);
create index if not exists idx_secondary_cnaes_cnae_code on establishment_secondary_cnaes (cnae_code);
create index if not exists idx_simples_flag on simples_options (simples_option_flag);
create index if not exists idx_simples_mei_flag on simples_options (mei_option_flag);
create index if not exists idx_import_plans_status on import_plans (status);
create index if not exists idx_import_plan_files_plan_id on import_plan_files (plan_id);
create index if not exists idx_import_plan_files_dataset on import_plan_files (dataset);
create index if not exists idx_import_checkpoints_status on import_checkpoints (status);
create index if not exists idx_import_checkpoints_dataset on import_checkpoints (dataset);
create index if not exists idx_import_quarantine_dataset on import_quarantine (dataset);
create index if not exists idx_import_quarantine_file_path on import_quarantine (file_path);
create index if not exists idx_import_quarantine_error_category on import_quarantine (error_category);
create index if not exists idx_import_quarantine_can_retry_later on import_quarantine (can_retry_later);

commit;
