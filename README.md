# CNPJ DB Loader

CNPJ DB Loader is a practical CLI for preparing Brazilian Federal Revenue CNPJ datasets for PostgreSQL.

## Current scope

This version focuses on the real loading workflow:

- inspect a downloaded directory
- extract Receita Federal ZIP archives
- validate an extracted tree
- sanitize validated files before import to remove known low-level byte issues
- print or generate the SQL schema
- configure and test the default PostgreSQL URL
- import validated dataset files into PostgreSQL with:
  - streaming batches
  - conflict-safe deduplication
  - checkpoint-based resume by file and byte offset
- row quarantine for invalid or constraint-breaking records without stopping the import
  - exact preparatory scanning for total rows and total batches before import starts
  - persisted import plans reused on resume for the same validated input and batch size
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
cnpj-db-loader db set "postgresql://user:password@localhost:5432/cnpj"
cnpj-db-loader schema generate
cnpj-db-loader import ./downloads/sanitized --batch-size 500 --verbose-progress
```

## Stable commands

```bash
cnpj-db-loader inspect <input>
cnpj-db-loader extract <input> [--output <path>]
cnpj-db-loader validate <input>
cnpj-db-loader sanitize <input> [--output <path>] [--dataset <name>] [-f]
cnpj-db-loader schema print
cnpj-db-loader schema generate [--name <name>] [--output <path>]
cnpj-db-loader db set <url>
cnpj-db-loader db show
cnpj-db-loader db test [--db-url <url>]
cnpj-db-loader db reset [--yes]
cnpj-db-loader import <input> [--db-url <url>] [--dataset <name>] [--batch-size <size>] [--verbose-progress] [-f]
cnpj-db-loader doctor [--input <path>] [--db-url <url>]
cnpj-db-loader quarantine stats [--dataset <name>] [--category <name>] [--stage <name>] [--retryable] [--terminal]
cnpj-db-loader quarantine list [--dataset <name>] [--category <name>] [--stage <name>] [--retryable] [--terminal] [--limit <number>] [--after-id <id>]
cnpj-db-loader quarantine show <id> [--db-url <url>]
```

## Logs

JSON execution logs are written inside the user home directory at `~/.cnpjdbloader/logs`.

For `import`, the CLI now also writes an incremental JSONL progress log with one event per committed batch, retry fallback, dataset metrics, file metrics, file failure, and final completion summary.

The final import summary now includes baseline timing and throughput metrics such as preparatory scan duration, execution duration, insert time, retry time, quarantine time, rows per second, and batches per minute.

`import --verbose-progress` shows a fixed multi-line status block instead of spamming the terminal with a new line on every progress update.

## Documentation

- [Usage](./docs/usage.md)
- [Architecture](./docs/architecture.md)
- [Commands](./docs/commands.md)
- [Quarantine](./docs/quarantine.md)
- [Sanitize](./docs/sanitize.md)
