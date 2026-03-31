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
- an optional sanitize step that removes known low-level byte issues before import starts
- COPY-based staged writes for the large datasets followed by staged-to-final materialization
- conflict-safe upserts for the smaller domain datasets
- `import_plans` and `import_plan_files` to persist exact import plans and avoid recounting the same source files on resume
- `import_checkpoints` to resume a failed load without clearing the whole database
- `import_materialization_checkpoints` to resume staged-to-final consolidation by dataset and chunk
- `import_quarantine` to store invalid rows and continue long-running imports
- a dedicated `quarantine` service to inspect quarantine rows without touching the import pipeline
- conservative load units to reduce memory pressure and prevent giant rollbacks
- compatibility with simplified final schemas that keep derived identifiers as regular columns when needed

## Import modules

The importer is now split into focused modules so future performance work can replace parts of the pipeline without rewriting the whole command:

- `planner`: selects datasets, collects source files, reuses or creates persisted import plans
- `source-reader`: streams validated files by byte offset for resume-safe reads
- `parser`: converts raw Receita lines into delimited field arrays
- `normalizer`: validates field counts and transforms parsed rows into database-ready records
- `staging-writer`: chooses the current write target and uses COPY for staged bulk loads
- `materializer`: consolidates staged datasets into the final relational schema with ordered upserts and resumable chunk checkpoints
- the materializer now reconciles missing lookup/domain codes from staged datasets before final upserts so late foreign-key failures do not stop the consolidation flow on placeholder-compatible domains
- materialization progress is now exposed explicitly to the CLI progress reporter and to JSONL heartbeat logs so long-running final upserts do not look stalled
- `finalizer`: centralizes performance tracking and import summary generation
- `checkpoint-manager`: owns checkpoint resume, persistence, and failed-file markers
- `quarantine-writer`: stores bad rows without stopping long imports
- `runner`: orchestrates the current import flow while keeping the service entry point small

The project now also generates dedicated staging tables for large datasets. The CLI exposes both a one-shot command (`import`) and split commands (`import load`, `import materialize`). Staging cleanup is handled explicitly through `database cleanup staging`. The write path sends the heavy datasets to staging tables first with only light normalization, then consolidates them into a simplified final schema in dependency order while keeping the smaller catalog datasets on the final schema directly. The final schema now stays closer to the Receita layout so the API can derive richer views later without forcing every first load to pay that cost inside PostgreSQL.

## Staging schema

The generated SQL schema supports lightweight `staging_*` tables for the large datasets that now move through the staged bulk-load flow before controlled final materialization.

These staging tables are intentionally:

- `UNLOGGED` for faster write-heavy workloads
- free of foreign keys and secondary indexes
- free of generated columns and upsert-only constraints
- shaped to mirror the validated dataset rows with minimal insert overhead
- equipped with `staging_id` so the materializer can checkpoint chunk progress safely

## Current execution flow

```text
inspect -> extract -> validate -> sanitize -> db/schema -> import
```

## Internal import flow

```text
planner -> source-reader -> parser -> normalizer -> staging-writer -> materializer -> finalizer
                  |                              |
                  +-> checkpoint-manager         +-> quarantine-writer
                                                 +-> materialization-checkpoints
```
