# CNPJ DB Loader

CNPJ DB Loader is a practical CLI for preparing Brazilian Federal Revenue CNPJ datasets for PostgreSQL.

## Current scope

This version focuses on the real loading workflow:

- inspect a downloaded directory
- extract Receita Federal ZIP archives
- validate an extracted tree
- sanitize validated files before import to remove known low-level byte issues
- print or generate final, staging, or combined SQL schemas
- configure and test the default PostgreSQL URL
- import validated dataset files into PostgreSQL with:
  - exact preparatory scanning for total rows and total batches before import starts
  - persisted import plans reused on resume for the same validated input and batch size
  - staged bulk loads for the large datasets through PostgreSQL COPY
  - direct final-schema upserts for the smaller domain datasets
  - checkpoint-based resume by file and byte offset
  - row quarantine for invalid or constraint-breaking records without stopping the import
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
cnpj-db-loader inspect ./downloads
cnpj-db-loader extract ./downloads
cnpj-db-loader validate ./downloads/extracted
cnpj-db-loader sanitize ./downloads/extracted
cnpj-db-loader database config set "postgresql://user:password@localhost:5432/cnpj"
cnpj-db-loader schema generate --profile full
cnpj-db-loader import ./downloads/sanitized --load-batch-size 500 --materialize-batch-size 50000 --verbose-progress
```

## Stable commands

```bash
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
cnpj-db-loader import <input> [--db-url <url>] [--dataset <name>] [--load-batch-size <size>] [--materialize-batch-size <size>] [--verbose-progress] [-f]
cnpj-db-loader import load <input> [--db-url <url>] [--dataset <name>] [--load-batch-size <size>] [--verbose-progress] [-f]
cnpj-db-loader import materialize <input> [--db-url <url>] [--dataset <name>] [--materialize-batch-size <size>] [--verbose-progress] [-f]
cnpj-db-loader doctor [--input <path>] [--db-url <url>]
cnpj-db-loader quarantine stats [--dataset <name>] [--category <name>] [--stage <name>] [--retryable] [--terminal]
cnpj-db-loader quarantine list [--dataset <name>] [--category <name>] [--stage <name>] [--retryable] [--terminal] [--limit <number>] [--after-id <id>]
cnpj-db-loader quarantine show <id> [--db-url <url>]
```

## Logs

JSON execution logs are written inside the user home directory at `~/.cnpjdbloader/logs`.

Every JSON and JSONL log entry now includes a structured envelope with fields such as `timestamp`, `level`, `severity`, `event`, and `kind`. Command success logs are written with `status: "success"`, command failures are written with `status: "failure"`, and incremental import progress events are classified with levels such as `debug`, `info`, `warning`, and `error`.

For `import`, the CLI now also writes an incremental JSONL progress log with one event per committed batch, retry fallback, dataset metrics, file metrics, file failure, final completion summary, and top-level import failure when execution aborts early.

The final import summary now includes baseline timing and throughput metrics such as preparatory scan duration, execution duration, insert time, retry time, quarantine time, rows per second, and batches per minute.

The import internals are now split into dedicated modules such as planner, source reader, parser, normalizer, checkpoint manager, quarantine writer, staging writer, materializer, and finalizer so staged bulk-load and final materialization changes can be implemented without rewriting the whole import command.

The CLI now exposes a split workflow as well: `import` runs the full pipeline, `import load` stops after staging/direct writes, `import materialize` resumes from the saved plan and pushes staged rows into the final tables, and `database cleanup ...` exposes safe maintenance commands for staging tables, final materialized tables, checkpoints, and saved plans.

Materialization progress is now checkpointed separately from file-load checkpoints, and the materializer works in resumable chunks controlled by `--materialize-batch-size`. During long final materialization steps, the CLI keeps the live progress output on a dedicated MATERIALIZING stage and the JSONL progress log emits periodic heartbeat entries so long-running staging-to-final upserts remain visible. Secondary CNAE expansion and partner dedupe-key derivation now happen during materialization instead of inside the initial staged write path.

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
