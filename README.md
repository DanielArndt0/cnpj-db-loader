# CNPJ DB Loader

CNPJ DB Loader is a practical CLI for preparing Brazilian Federal Revenue CNPJ datasets for PostgreSQL.

## Current scope

This version focuses on the real loading workflow:

- inspect a downloaded directory
- check, download, retry, clean, and inspect the latest Federal Revenue CNPJ monthly ZIP archives from the public share
- extract Receita Federal ZIP archives
- validate an extracted tree
- sanitize validated files before import to remove known low-level byte issues
- print or generate final, staging, or combined SQL schemas
- configure and test the default PostgreSQL URL
- import validated dataset files into PostgreSQL with:
  - exact preparatory scanning for total rows and total batches before import starts
  - persisted import plans reused on resume for the same validated input and batch size
  - staged bulk loads for the large datasets through PostgreSQL COPY
  - automatic materialization of `establishment_secondary_cnaes` from establishment secondary CNAE data
  - direct final-schema upserts for the smaller domain datasets
  - checkpoint-based resume by file and byte offset
  - row quarantine for invalid or constraint-breaking records without stopping the import
- generate a direct `psql` import script that loads sanitized Receita files without rewriting the full dataset into another CSV tree
- quarantine inspection commands for analyzing rows stored in `import_quarantine`

## Installation

```bash
npm install
```

During development:

```bash
npm run cli -- --help
```

## Quick start

```bash
cnpj-db-loader federal-revenue check
cnpj-db-loader federal-revenue download --output ./downloads
cnpj-db-loader federal-revenue status --output ./downloads
cnpj-db-loader inspect ./downloads/<reference>
cnpj-db-loader extract ./downloads/<reference>
cnpj-db-loader validate ./downloads/<reference>/extracted
cnpj-db-loader sanitize ./downloads/<reference>/extracted
cnpj-db-loader database config set "postgresql://user:password@localhost:5432/cnpj"
cnpj-db-loader schema generate --profile full
cnpj-db-loader import ./downloads/<reference>/sanitized --load-batch-size 500 --materialize-batch-size 50000 --verbose-progress

# Optional hybrid path for PostgreSQL direct loading
cnpj-db-loader postgres generate-script ./downloads/<reference>/sanitized --output ./downloads/<reference>/postgres-direct --force
psql "postgres://postgres:postgres@localhost:5432/cnpj" -f ./downloads/<reference>/postgres-direct/import-postgres-direct.sql
```

## Stable commands

```bash
cnpj-db-loader federal-revenue check [reference] [--reference <yyyy-mm>] [--current]
cnpj-db-loader federal-revenue download [reference] [--reference <yyyy-mm>] [--current] [--output <path>] [--retries <number>] [--overwrite] [-f]
cnpj-db-loader federal-revenue status [reference] [--reference <yyyy-mm>] [--current] [--output <path>]
cnpj-db-loader federal-revenue retry [reference] [--reference <yyyy-mm>] [--current] [--output <path>] [--retries <number>] [--overwrite] [-f]
cnpj-db-loader federal-revenue clean [reference] [--reference <yyyy-mm>] [--current] [--output <path>] [--partials | --failed | --all] [-f]
cnpj-db-loader federal-revenue sync [reference] [--reference <yyyy-mm>] [--current] [--output <path>] [--extract-output <path>] [--sanitize-output <path>] [--db-url <url>] [--dataset <name>] [--load-batch-size <size>] [--materialize-batch-size <size>] [--verbose-progress] [--force-lock] [-f]
cnpj-db-loader inspect <input>
cnpj-db-loader extract <input> [--output <path>]
cnpj-db-loader validate <input>
cnpj-db-loader sanitize <input> [--output <path>] [--dataset <name>] [-f]
cnpj-db-loader schema print [--profile <profile>]
cnpj-db-loader schema generate [--name <name>] [--output <path>] [--profile <profile>]
cnpj-db-loader database config set <url>
cnpj-db-loader database config show
cnpj-db-loader database config test [--db-url <url>]
cnpj-db-loader database config reset [--force]
cnpj-db-loader database cleanup staging [--db-url <url>] [--dataset <name>] [--validated-path <path>] [--force]
cnpj-db-loader database cleanup materialized [--db-url <url>] [--dataset <name>] [--force]
cnpj-db-loader database cleanup checkpoints [--db-url <url>] [--phase <phase>] [--dataset <name>] [--validated-path <path>] [--plan-id <id>] [--force]
cnpj-db-loader database cleanup plans [--db-url <url>] [--validated-path <path>] [--plan-id <id>] [--force]
cnpj-db-loader postgres generate-script <input> [--output <path>] [--dataset <name>] [--script-name <name>] [--source-encoding <encoding>] [-f]
cnpj-db-loader postgres export-csv <input> [--output <path>] [--dataset <name>] [--script-name <name>] [-f]
cnpj-db-loader import <input> [--db-url <url>] [--dataset <name>] [--load-batch-size <size>] [--materialize-batch-size <size>] [--verbose-progress] [-f]
cnpj-db-loader import load <input> [--db-url <url>] [--dataset <name>] [--load-batch-size <size>] [--verbose-progress] [-f]
cnpj-db-loader import materialize <input> [--db-url <url>] [--dataset <name>] [--materialize-batch-size <size>] [--verbose-progress] [-f]
cnpj-db-loader doctor [--input <path>] [--db-url <url>]
cnpj-db-loader quarantine stats [--dataset <name>] [--category <name>] [--stage <name>] [--retryable] [--terminal]
cnpj-db-loader quarantine list [--dataset <name>] [--category <name>] [--stage <name>] [--retryable] [--terminal] [--limit <number>] [--after-id <id>]
cnpj-db-loader quarantine show <id> [--db-url <url>]
```

## PostgreSQL direct import workflow

For local benchmarks or controlled full loads, the CLI can now generate a direct `psql` import script after sanitization:

```bash
cnpj-db-loader sanitize ./downloads/<reference>/extracted
cnpj-db-loader postgres generate-script ./downloads/<reference>/sanitized --output ./downloads/<reference>/postgres-direct --force
psql "postgres://postgres:postgres@localhost:5432/cnpj" -f ./downloads/<reference>/postgres-direct/import-postgres-direct.sql
```

This path keeps download, extraction, validation and sanitization inside the loader, then lets PostgreSQL load the sanitized Receita files directly through `\copy`, convert values into staging tables and materialize the final tables with set-based SQL. The standard `import` command remains the safest path when checkpoint resume and quarantine recovery are required.

## Logs

JSON execution logs are written inside the user home directory at `~/.cnpjdbloader/logs`.

Every JSON and JSONL log entry now includes a structured envelope with fields such as `timestamp`, `level`, `severity`, `event`, and `kind`. Command success logs are written with `status: "success"`, command failures are written with `status: "failure"`, and incremental import progress events are classified with levels such as `debug`, `info`, `warning`, and `error`.

For `import`, the CLI now also writes an incremental JSONL progress log with one event per committed batch, retry fallback, dataset metrics, file metrics, file failure, final completion summary, and top-level import failure when execution aborts early.

The final import summary now includes baseline timing and throughput metrics such as preparatory scan duration, execution duration, insert time, retry time, quarantine time, rows per second, and batches per minute.

The import internals are now split into dedicated modules such as planner, source reader, parser, normalizer, checkpoint manager, quarantine writer, staging writer, materializer, and finalizer so staged bulk-load and final materialization changes can be implemented without rewriting the whole import command.

The CLI now exposes a split workflow as well: `import` runs the full pipeline, `import load` stops after staging/direct writes, `import materialize` resumes from the saved plan and pushes staged rows into the final tables, and `database cleanup ...` exposes safe maintenance commands for staging tables, simplified final materialized tables, checkpoints, and saved plans.

Materialization progress is now checkpointed separately from file-load checkpoints, and the materializer works in resumable chunks controlled by `--materialize-batch-size`. During long final materialization steps, the CLI keeps the live progress output on a dedicated MATERIALIZING stage while reducing per-chunk checkpoint and JSONL write overhead so resumable chunks stay fast. The simplified final schema keeps raw secondary CNAE text in establishments and also materializes `establishment_secondary_cnaes` so APIs can query one row per secondary CNAE without running a separate backfill script.

The Federal Revenue commands write the same structured command logs and keep the remote-download phase outside the import internals. Existing completed ZIP files are skipped by default, temporary `.part` files are used while downloads are still in progress, and each reference keeps a local manifest for `status`, `retry`, `clean`, and future runner automation.

The generated database schema now supports three profiles:

- `full`: final relational tables, import control tables, and staging tables
- `final`: only the final relational and control tables
- `staging`: only the lightweight staging tables used by the staged bulk-load flow

`import --verbose-progress` shows a fixed multi-line status block instead of spamming the terminal with a new line on every progress update.

## Documentation

- [Usage](./docs/usage.md)
- [Architecture](./docs/architecture.md)
- [Commands](./docs/commands.md)
- [Quarantine](./docs/quarantine.md)
- [Sanitize](./docs/sanitize.md)
- [Federal Revenue](./docs/federal-revenue.md)
- [PostgreSQL Direct Import](./docs/postgres-direct.md)

- Materialization now stores lightweight staging validation markers (row count and max staging id) in the materialization checkpoint table so reruns can verify the live staging state quickly and reuse lookup reconciliation when the staging snapshot is unchanged. The runtime validates that the required import tables already exist but no longer creates or alters them automatically.
