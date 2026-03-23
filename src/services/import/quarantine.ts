import { Client } from "pg";

import type { ImportDatasetType } from "./types.js";

export async function ensureQuarantineTable(client: Client): Promise<void> {
  await client.query(`
    create table if not exists import_quarantine (
      id bigserial primary key,
      dataset text not null,
      file_path text not null,
      row_number bigint,
      checkpoint_offset bigint,
      error_code text,
      error_message text not null,
      raw_line text not null,
      parsed_payload jsonb,
      created_at timestamptz not null default now()
    )
  `);
  await client.query(
    `create index if not exists idx_import_quarantine_dataset on import_quarantine (dataset)`,
  );
  await client.query(
    `create index if not exists idx_import_quarantine_file_path on import_quarantine (file_path)`,
  );
}

export type QuarantineInput = {
  dataset: ImportDatasetType;
  filePath: string;
  rowNumber: number;
  checkpointOffset: number;
  rawLine: string;
  error: unknown;
  parsedPayload?: Record<string, unknown> | null;
};

export async function writeQuarantineRow(
  client: Client,
  input: QuarantineInput,
): Promise<void> {
  const errorCode =
    typeof input.error === "object" && input.error && "code" in input.error
      ? String((input.error as { code?: string }).code ?? "QUARANTINED_ROW")
      : "QUARANTINED_ROW";
  const errorMessage =
    input.error instanceof Error ? input.error.message : String(input.error);

  await client.query(
    `insert into import_quarantine (
       dataset,
       file_path,
       row_number,
       checkpoint_offset,
       error_code,
       error_message,
       raw_line,
       parsed_payload,
       created_at
     ) values ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, now())`,
    [
      input.dataset,
      input.filePath,
      input.rowNumber,
      input.checkpointOffset,
      errorCode,
      errorMessage,
      input.rawLine,
      input.parsedPayload ? JSON.stringify(input.parsedPayload) : null,
    ],
  );
}
