export function createIndexesSql(): string {
  return [
    "-- Operational indexes",
    "create index if not exists idx_establishments_cnpj_root on establishments (cnpj_root);",
    "create index if not exists idx_partners_cnpj_root on partners (cnpj_root);",
    "create index if not exists idx_import_plans_status on import_plans (status);",
    "create index if not exists idx_import_plans_load_status on import_plans (load_status);",
    "create index if not exists idx_import_plans_materialization_status on import_plans (materialization_status);",
    "create index if not exists idx_import_plan_files_plan_id on import_plan_files (plan_id);",
    "create index if not exists idx_import_plan_files_dataset on import_plan_files (dataset);",
    "create index if not exists idx_import_checkpoints_status on import_checkpoints (status);",
    "create index if not exists idx_import_materialization_checkpoints_status on import_materialization_checkpoints (status);",
    "create index if not exists idx_import_materialization_checkpoints_plan_id on import_materialization_checkpoints (plan_id);",
    "create index if not exists idx_import_materialization_checkpoints_dataset on import_materialization_checkpoints (dataset);",
    "create index if not exists idx_import_checkpoints_dataset on import_checkpoints (dataset);",
    "create index if not exists idx_import_quarantine_dataset on import_quarantine (dataset);",
    "create index if not exists idx_import_quarantine_file_path on import_quarantine (file_path);",
    "create index if not exists idx_import_quarantine_error_category on import_quarantine (error_category);",
    "create index if not exists idx_import_quarantine_can_retry_later on import_quarantine (can_retry_later);",
  ].join("\n");
}
