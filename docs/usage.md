# Usage

## Recommended flow

```bash
cnpj-db-loader inspect ./downloads
cnpj-db-loader extract ./downloads
cnpj-db-loader validate ./downloads/extracted
cnpj-db-loader db set "postgresql://user:password@localhost:5432/cnpj"
cnpj-db-loader schema generate
cnpj-db-loader import ./downloads/extracted --batch-size 500 --verbose-progress
```

## What each step does

| Step | Command                    | Purpose                                                                                                          |
| ---- | -------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| 1    | `inspect <input>`          | Detect whether the folder contains ZIP archives, extracted content, or both                                      |
| 2    | `extract <input>`          | Extract every Receita ZIP archive into `./extracted` by default                                                  |
| 3    | `validate <input>`         | Validate the extracted dataset tree and confirm that the required dataset blocks are present                     |
| 4    | `db show` / `db set <url>` | Review or configure the PostgreSQL connection                                                                    |
| 5    | `schema generate`          | Generate the SQL schema, including `import_checkpoints` and `import_quarantine`                                  |
| 6    | `import <input>`           | Import validated files with streaming batches, conflict-safe upserts, checkpoint resume, and quarantine fallback |

## Important behavior of import

`import` is designed to be safe for large datasets.

- it starts with an exact preparatory scan that counts source rows and planned batches
- it reads files in streaming mode
- it commits per batch instead of holding one giant transaction
- it stores progress in `import_checkpoints`
- rows are first retried with known sanitization rules (for example, removing NUL bytes). Only rows that still fail are written to `import_quarantine` and skipped
- if a batch fails, rerunning the same command resumes from the last committed byte offset
- it stays idempotent for the current schema, so rerunning the same files does not create duplicate rows

## Recommended import settings

For large first loads, start with:

```bash
cnpj-db-loader import ./downloads/extracted --batch-size 500 --verbose-progress
```

Increase the batch size only after you confirm that your PostgreSQL instance and Docker memory budget can handle it.

## PostgreSQL and Docker recommendations

For a machine with 32 GB RAM, start conservatively:

- `shared_buffers = 512MB` to `1GB`
- `work_mem = 8MB` to `16MB`
- `maintenance_work_mem = 256MB`
- make sure Docker Desktop is not over-allocating memory to the container

These are starting points, not absolute rules. The safest optimization is still keeping `--batch-size` modest.

## Import progress visibility

`import` writes two kinds of logs:

- a final JSON summary log
- an incremental JSONL progress log for every committed batch

Use `--verbose-progress` when you want a fixed multi-line status block with dataset, file, committed rows, total batches, and file progress while the import is running.

The exact preparatory scan runs again on resume. The importer then reuses the checkpoint table to continue from the last committed byte offset instead of restarting the data load itself. Rows first go through a small sanitization pipeline for known recoverable problems. Only rows that still fail after retry are written to `import_quarantine`, so a few bad rows do not stop the entire dataset.

## Quarantine behavior

The quarantine flow is now designed to be reusable for future recovery commands.

Rows sent to `import_quarantine` keep extra metadata such as:

- `error_category`
- `error_stage`
- `sanitizations_applied`
- `retry_count`
- `can_retry_later`

This allows future commands to replay only recoverable rows after new rules are added.
