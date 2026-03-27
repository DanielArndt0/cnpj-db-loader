export function createImportPlansSql(): string {
  return [
    "create table if not exists import_plans (",
    "  id bigserial primary key,",
    "  source_fingerprint text not null unique,",
    "  input_path text not null,",
    "  validated_path text not null,",
    "  batch_size integer not null,",
    "  target_database text not null,",
    "  total_datasets integer not null,",
    "  total_files integer not null,",
    "  total_rows bigint not null,",
    "  total_batches bigint not null,",
    "  execution_order jsonb not null,",
    "  status text not null default 'planned',",
    "  load_status text not null default 'pending',",
    "  materialization_status text not null default 'pending',",
    "  last_phase text,",
    "  last_error text,",
    "  created_at timestamp with time zone not null default now(),",
    "  updated_at timestamp with time zone not null default now(),",
    "  last_used_at timestamp with time zone not null default now()",
    ");",
  ].join("\n");
}

export function createImportPlanFilesSql(): string {
  return [
    "create table if not exists import_plan_files (",
    "  id bigserial primary key,",
    "  plan_id bigint not null references import_plans (id) on delete cascade,",
    "  dataset text not null,",
    "  dataset_index integer not null,",
    "  file_index integer not null,",
    "  file_path text not null,",
    "  file_display_path text not null,",
    "  file_size bigint not null,",
    "  file_mtime timestamp with time zone not null,",
    "  total_rows bigint not null,",
    "  total_batches bigint not null,",
    "  unique (plan_id, file_path)",
    ");",
  ].join("\n");
}

export function createImportCheckpointsSql(): string {
  return [
    "create table if not exists import_checkpoints (",
    "  id bigserial primary key,",
    "  dataset text not null,",
    "  file_path text not null,",
    "  file_size bigint not null,",
    "  file_mtime timestamp with time zone not null,",
    "  byte_offset bigint not null default 0,",
    "  rows_committed bigint not null default 0,",
    "  status text not null default 'pending',",
    "  last_error text,",
    "  updated_at timestamp with time zone not null default now(),",
    "  unique (dataset, file_path)",
    ");",
  ].join("\n");
}

export function createImportMaterializationCheckpointsSql(): string {
  return [
    "create table if not exists import_materialization_checkpoints (",
    "  id bigserial primary key,",
    "  plan_id bigint not null references import_plans (id) on delete cascade,",
    "  dataset text not null,",
    "  target_table text not null,",
    "  status text not null default 'pending',",
    "  rows_materialized bigint not null default 0,",
    "  last_staging_id bigint not null default 0,",
    "  chunks_completed bigint not null default 0,",
    "  last_error text,",
    "  started_at timestamp with time zone,",
    "  completed_at timestamp with time zone,",
    "  updated_at timestamp with time zone not null default now(),",
    "  unique (plan_id, dataset)",
    ");",
  ].join("\n");
}

export function createImportQuarantineSql(): string {
  return [
    "create table if not exists import_quarantine (",
    "  id bigserial primary key,",
    "  dataset text not null,",
    "  file_path text not null,",
    "  row_number bigint,",
    "  checkpoint_offset bigint,",
    "  error_code text,",
    "  error_category text,",
    "  error_stage text,",
    "  error_message text not null,",
    "  raw_line text not null,",
    "  parsed_payload jsonb,",
    "  sanitizations_applied jsonb,",
    "  retry_count integer not null default 0,",
    "  can_retry_later boolean not null default false,",
    "  created_at timestamp with time zone not null default now()",
    ");",
  ].join("\n");
}

export function createControlSchemaParts(): string[] {
  return [
    "-- Import control tables",
    createImportPlansSql(),
    createImportPlanFilesSql(),
    createImportCheckpointsSql(),
    createImportMaterializationCheckpointsSql(),
    createImportQuarantineSql(),
  ];
}
