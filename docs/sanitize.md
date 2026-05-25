# Sanitize

## Purpose

`sanitize` prepares a clean dataset tree before PostgreSQL import.

The command now performs robust text sanitization for Receita Federal files:

- reads legacy Receita files using a configurable source encoding;
- writes sanitized output as clean UTF-8;
- removes NUL bytes;
- removes invalid bytes that cannot be safely decoded;
- removes problematic control characters while preserving line breaks;
- keeps the original dataset file names and directory structure.

This makes the sanitized tree safer for both the standard loader import flow and the hybrid PostgreSQL direct import flow.

## Command

```bash
cnpj-db-loader sanitize <input>
```

## Options

| Option                         | Description                                                                                                           |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------- |
| `--output <path>`              | Custom output directory for the sanitized dataset tree.                                                               |
| `--dataset <name>`             | Sanitize only one dataset block, such as `establishments` or `companies`.                                             |
| `--source-encoding <encoding>` | Source file encoding used while reading Receita files. Defaults to `WIN1252`. Supported: `WIN1252`, `LATIN1`, `UTF8`. |
| `-f, --force`                  | Skip the confirmation prompt.                                                                                         |

## Default output behavior

- when the validated path is `.../extracted`, the default sanitized output is `.../sanitized`;
- otherwise the default output is `<validated-path>-sanitized`.

## Recommended standard flow

```bash
cnpj-db-loader inspect ./downloads
cnpj-db-loader extract ./downloads
cnpj-db-loader validate ./downloads/extracted
cnpj-db-loader sanitize ./downloads/extracted
cnpj-db-loader import ./downloads/sanitized --load-batch-size 500 --materialize-batch-size 50000 --verbose-progress
```

## Recommended hybrid PostgreSQL flow

Because sanitized files are now written as UTF-8, the direct PostgreSQL script can use `UTF8` as the source encoding.

```bash
cnpj-db-loader sanitize ./downloads/extracted --output ./downloads/sanitized --force
cnpj-db-loader postgres generate-script ./downloads/sanitized --output ./downloads/postgres-direct --source-encoding UTF8 --force
psql -d "postgres://postgres:postgres@localhost:5432/cnpj" -f ./downloads/postgres-direct/import-postgres-direct.sql
```

## What it improves

- fewer encoding-related `COPY` failures;
- fewer UTF-8 / NUL-byte related insert failures;
- no invalid bytes in sanitized output;
- fewer problematic control characters in PostgreSQL input files;
- less row-by-row fallback during standard import;
- better throughput for large datasets;
- cleaner quarantine data because known low-level issues are removed earlier.

## Encoding notes

The default source encoding is `WIN1252`, which matches the common legacy encoding used by Receita files.

If a source dataset still fails because of undefined Windows-1252 bytes, `LATIN1` can be used as a more permissive decoder:

```bash
cnpj-db-loader sanitize ./downloads/extracted --source-encoding LATIN1 --output ./downloads/sanitized --force
```

The output is still UTF-8 in both cases.

## Notes

- `sanitize` does not replace validation; it assumes the dataset tree is already valid.
- `sanitize` preserves file names and relative paths so existing import logic can keep detecting datasets by name.
- `import` still keeps quarantine and retry logic for unexpected issues that survive sanitization.
- no database schema changes are required to use `sanitize`.
