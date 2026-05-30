# Sanitize

## Purpose

`sanitize` prepares a normalized dataset tree before PostgreSQL import.

Receita Federal source files can use the legacy `ISO-8859-1` encoding. The command now normalizes source bytes before any downstream text parsing:

```txt
raw extracted file
  -> byte stream
  -> remove NUL bytes (0x00)
  -> decode source encoding
  -> encode as UTF-8
  -> write temporary UTF-8 file
  -> validate normalized output
  -> replace destination file only after validation succeeds
```

This prevents accented Portuguese characters from being persisted incorrectly before import.

## Command

```bash
cnpj-db-loader sanitize <input>
```

## Options

| Option                         | Description                                                                                                                         |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| `--output <path>`              | Custom output directory for the sanitized dataset tree.                                                                             |
| `--dataset <name>`             | Sanitize only one dataset block, such as `establishments` or `companies`.                                                           |
| `--source-encoding <encoding>` | Source file encoding used while reading Receita files. Defaults to `LATIN1` (`ISO-8859-1`). Supported: `LATIN1`, `WIN1252`, `UTF8`. |
| `--allow-replacement-chars`    | Allow Unicode replacement characters (`�`) in output instead of failing validation. Use only for manual inspection.                 |
| `-f, --force`                  | Skip the confirmation prompt.                                                                                                       |

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

Sanitized files are validated UTF-8 output, so the direct PostgreSQL script should use `UTF8` as the source encoding.

```bash
cnpj-db-loader sanitize ./downloads/extracted --output ./downloads/sanitized --force
cnpj-db-loader postgres generate-script ./downloads/sanitized --output ./downloads/postgres-direct --source-encoding UTF8 --force
psql -d "postgres://postgres:postgres@localhost:5432/cnpj" -f ./downloads/postgres-direct/import-postgres-direct.sql
```

## Safe normalization behavior

The sanitization pipeline:

- processes large files as streams instead of loading them fully into memory;
- removes NUL bytes at byte level before decoding;
- defaults to decoding Receita source files as `ISO-8859-1`;
- writes UTF-8 output to a temporary file;
- validates the temporary output before replacing the destination file;
- preserves a previously valid destination file when normalization or validation fails;
- rejects Unicode replacement characters (`�`) by default instead of silently removing them;
- reports source encoding, removed NUL bytes, removed control characters and replacement-character metrics.

## Encoding notes

The default source encoding is `LATIN1`, which maps to `ISO-8859-1` and matches the Receita files validated during development.

Use `WIN1252` only when processing a source tree that is known to use Windows-1252:

```bash
cnpj-db-loader sanitize ./downloads/extracted --source-encoding WIN1252 --output ./downloads/sanitized --force
```

Use `UTF8` only when the input tree is already valid UTF-8:

```bash
cnpj-db-loader sanitize ./downloads/extracted --source-encoding UTF8 --output ./downloads/sanitized --force
```

## Replacement-character validation

The visible character:

```txt
�
```

is the Unicode replacement character. Once it has replaced an original accented character and the corrupted content has been persisted, the original value cannot be reliably recovered from that file alone.

For this reason, sanitization fails by default if replacement characters remain in normalized output. The optional `--allow-replacement-chars` flag exists only for controlled manual inspection and should not be used in normal import flows.

## Notes

- `sanitize` does not replace dataset-tree validation;
- `sanitize` preserves file names and relative paths so existing import logic can keep detecting datasets by name;
- row-level business inconsistencies that survive normalization remain handled by the existing import quarantine flow;
- no database schema changes are required to use `sanitize`.
