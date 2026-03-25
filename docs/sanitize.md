# Sanitize

## Purpose

`sanitize` prepares a clean dataset tree before PostgreSQL import.

It removes known low-level byte issues, especially `0x00` / NUL bytes, from validated dataset files and writes the result to a new output directory. The goal is to reduce slow fallback work during import so PostgreSQL receives cleaner files from the start.

## Command

```bash
cnpj-db-loader sanitize <input>
```

## Options

| Option             | Description                                                               |
| ------------------ | ------------------------------------------------------------------------- |
| `--output <path>`  | Custom output directory for the sanitized dataset tree.                   |
| `--dataset <name>` | Sanitize only one dataset block, such as `establishments` or `companies`. |
| `-f, --force`      | Skip the confirmation prompt.                                             |

## Default output behavior

- when the validated path is `.../extracted`, the default sanitized output is `.../sanitized`
- otherwise the default output is `<validated-path>-sanitized`

## Recommended flow

```bash
cnpj-db-loader inspect ./downloads
cnpj-db-loader extract ./downloads
cnpj-db-loader validate ./downloads/extracted
cnpj-db-loader sanitize ./downloads/extracted
cnpj-db-loader import ./downloads/sanitized --batch-size 500 --verbose-progress
```

## What it improves

- fewer UTF-8 / NUL-byte related insert failures
- less row-by-row fallback during import
- better import throughput for large datasets
- cleaner quarantine data because known low-level issues are removed earlier

## Notes

- `sanitize` does not replace validation; it assumes the dataset tree is already valid
- `import` still keeps quarantine and retry logic for unexpected issues that survive sanitization
- no database schema changes are required to use `sanitize`
