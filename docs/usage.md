# Usage

## Recommended flow

```bash
cnpj-db-loader inspect ./downloads
cnpj-db-loader extract ./downloads
cnpj-db-loader validate ./downloads/extracted
cnpj-db-loader sanitize ./downloads/extracted
cnpj-db-loader database config set "postgresql://user:password@localhost:5432/cnpj"
cnpj-db-loader schema generate --profile full
cnpj-db-loader import ./downloads/sanitized --load-batch-size 500 --materialize-batch-size 50000 --verbose-progress
cnpj-db-loader import load ./downloads/sanitized --load-batch-size 20000
cnpj-db-loader import materialize ./downloads/sanitized --materialize-batch-size 50000
```

## What each step does

| Step | Command                                              | Purpose                                                                                         |
| ---- | ---------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| 1    | `inspect <input>`                                    | Detect whether the folder contains ZIP archives, extracted content, or both                     |
| 2    | `extract <input>`                                    | Extract every Receita ZIP archive into `./extracted` by default                                 |
| 3    | `validate <input>`                                   | Validate the extracted dataset tree and confirm that the required dataset blocks are present    |
| 4    | `sanitize <input>`                                   | Prepare a sanitized dataset tree by removing known low-level byte issues before import          |
| 5    | `database config show` / `database config set <url>` | Review or configure the PostgreSQL connection                                                   |
| 6    | `schema generate --profile full`                     | Generate the combined SQL schema with final, control, and staging tables                        |
| 7    | `import <input>`                                     | Run the full pipeline: staged/direct load, staged materialization, and final summary generation |
| 8    | `import load <input>`                                | Stop after the load phase when you want staging populated without immediately materializing it  |
| 9    | `import materialize <input>`                         | Resume from the saved plan and materialize staged datasets into the final schema in chunks      |

## Schema profiles

Use the schema command profile that matches the database shape you want to prepare:

- `full`: final tables, import control tables, and staging tables
- `final`: only the final relational and control tables
- `staging`: only the lightweight `staging_*` tables used by the staged bulk-load steps before final materialization

Examples:

```bash
cnpj-db-loader schema generate --profile full
cnpj-db-loader schema generate --profile final
cnpj-db-loader schema generate --profile staging
```

## Important behavior of import

`import` is designed to be safe for large datasets. The CLI now also exposes `import load`, `import materialize`, and `database cleanup ...` so the heavy phases and safe reset operations can be automated separately.

- it starts with an exact preparatory scan that counts source rows and planned batches when no saved plan exists
- it persists the import plan in the database and reuses it on resume when the validated source files and batch size match
- it reads files in streaming mode
- it loads the large datasets into lightweight staging tables through PostgreSQL COPY with only light normalization in the hot path and defers heavier work to the materialization stage in dependency order
- before each staged dataset is materialized into the final schema, the importer now reconciles missing lookup/domain codes with placeholder rows when that dependency model supports it
- once the file import phase ends, the terminal switches to a dedicated MATERIALIZING stage and the JSONL progress log emits heartbeat entries during long staged-to-final upserts
- it still upserts the smaller domain datasets directly into the final schema
- it commits per load unit instead of holding one giant transaction
- it stores file-load progress in `import_checkpoints`
- it stores materialization progress in `import_materialization_checkpoints`
- rows that still fail validation or database constraints are written to `import_quarantine` and skipped
- if a batch fails, rerunning the same command resumes from the last committed byte offset
- new import plans truncate the selected staging tables before loading, while resumed plans reuse staged rows that already match saved checkpoints before the final materialization pass runs again

## Recommended import settings

For large first loads, sanitize first and then start with:

```bash
cnpj-db-loader sanitize ./downloads/extracted
cnpj-db-loader import ./downloads/sanitized --load-batch-size 500 --materialize-batch-size 50000 --verbose-progress
cnpj-db-loader import load ./downloads/sanitized --load-batch-size 20000
cnpj-db-loader import materialize ./downloads/sanitized --materialize-batch-size 50000
```

Increase `--load-batch-size` only after you confirm that your PostgreSQL instance and memory budget can handle larger COPY load units. Use `--materialize-batch-size` to control how many staged rows each materialization chunk processes before saving a materialization checkpoint. The saved import plan keeps the original load batch size used during planning/loading, so changing only `--materialize-batch-size` does not create a new plan; the UI now shows both values separately during resume/materialization runs.

## PostgreSQL and Docker recommendations

For a machine with 32 GB RAM, start conservatively:

- `shared_buffers = 512MB` to `1GB`
- `work_mem = 8MB` to `16MB`
- `maintenance_work_mem = 256MB`
- make sure Docker Desktop is not over-allocating memory to the container

These are starting points, not absolute rules. The safest optimization is still keeping `--load-batch-size` modest until you validate your PostgreSQL limits.

## Import progress visibility

`import` writes two kinds of logs inside `~/.cnpjdbloader/logs`:

- a final JSON summary log
- an incremental JSONL progress log for every committed batch, retry fallback, file metrics, dataset metrics, final completion summary, and top-level import failure when execution aborts early

Every JSON and JSONL log now carries a structured envelope with `timestamp`, `level`, `severity`, `event`, and `kind`. This makes it easier to filter informational events versus warnings and errors in JSON viewers and JSONL extensions.

Use `--verbose-progress` when you want a fixed multi-line status block with dataset, file, committed rows, total batches, and file progress while the import is running.

The final import summary also includes baseline metrics for preparatory scan time, execution time, insert time, retry time, quarantine time, materialization time, rows per second, and batches per minute.

The exact preparatory scan runs only when no saved import plan exists for the same validated source files and batch size. On resume, the importer reuses the saved plan and then reuses the checkpoint table to continue from the last committed byte offset instead of restarting the data load itself. Rows that fail after retries are written to `import_quarantine`, so a few bad rows do not stop the entire dataset. Running `sanitize` first reduces how often the importer has to fall back to those slower recovery paths.

## Quarantine analysis

Use the `quarantine` service after a long-running import when you want to inspect the rows that could not be inserted.

```bash
cnpj-db-loader quarantine stats
cnpj-db-loader quarantine list --dataset establishments --limit 20
cnpj-db-loader quarantine show 42
```

`quarantine stats` is useful for understanding the scale of a problem by dataset, error category, or error stage.

`quarantine list` is useful for paging through rows with filters such as `--retryable`, `--terminal`, `--category`, and `--stage`.

`quarantine show` loads one quarantined row in detail, including the raw line and parsed payload when available.

## Database maintenance commands

The `database` command family now separates connection configuration from destructive maintenance actions:

```bash
cnpj-db-loader database config show
cnpj-db-loader database cleanup staging --validated-path ./downloads/sanitized --force
cnpj-db-loader database cleanup materialized --dataset companies --force
cnpj-db-loader database cleanup checkpoints --phase all --validated-path ./downloads/sanitized --force
cnpj-db-loader database cleanup plans --validated-path ./downloads/sanitized --force
```

Use `--force` to skip confirmation prompts. Without it, cleanup commands always ask before changing the database.

- Materialization now stores lightweight staging validation markers (row count and max staging id) in the materialization checkpoint table so reruns can verify the live staging state quickly and reuse lookup reconciliation when the staging snapshot is unchanged.
