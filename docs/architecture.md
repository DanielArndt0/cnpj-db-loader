# Architecture

## What matters in this version

The current CLI is centered on one practical job: move Receita Federal CNPJ data from downloaded archives into PostgreSQL safely.

## Main layers

| Folder           | Purpose                                                        |
| ---------------- | -------------------------------------------------------------- |
| `src/cli`        | Command registration and terminal output                       |
| `src/services`   | Real application behavior used by the CLI                      |
| `src/dictionary` | Dataset layout definitions derived from the Receita dictionary |
| `src/core`       | Shared errors, prompts, and utilities                          |
| `src/config`     | Local configuration helpers and paths                          |

## Import design

The import pipeline now uses:

- deterministic dataset order to respect foreign keys
- an exact preparatory scan that counts total source rows and planned batches before the first write
- streaming file reads to avoid loading the full dataset into RAM
- conflict-safe inserts and upserts to avoid duplication
- `import_checkpoints` to resume a failed load without clearing the whole database
- `import_quarantine` to store invalid rows and continue long-running imports
- conservative batch commits to reduce memory pressure and prevent giant rollbacks
- compatibility with both generated and regular `partner_dedupe_key` schemas during partner imports

## Current execution flow

```text
inspect -> extract -> validate -> db/schema -> import
```
