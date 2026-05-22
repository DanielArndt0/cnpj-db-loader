# Federal Revenue integration

The `federal-revenue` command group adds the phase 1 remote dataset workflow for the Brazilian Federal Revenue CNPJ public share.

This feature is additive. It does not replace the stable local commands. The full `sync` command uses the same internal services already used by the manual flow:

```text
check/download -> extract -> validate -> sanitize -> import
```

The command also has the shorter alias `revenue`.

## Commands

```bash
cnpj-db-loader federal-revenue check
cnpj-db-loader federal-revenue download --output ./downloads --force
cnpj-db-loader federal-revenue sync --output ./downloads --db-url "postgresql://user:password@localhost:5432/cnpj" --force
```

Alias examples:

```bash
cnpj-db-loader revenue check
cnpj-db-loader revenue download --reference 2026-05 --output ./downloads --force
```

## Reference selection

By default, the CLI lists the public share and selects the latest available folder in the `YYYY-MM` format.

Use an explicit reference when you need a deterministic month:

```bash
cnpj-db-loader federal-revenue check --reference 2026-05
cnpj-db-loader federal-revenue check 2026-05
cnpj-db-loader federal-revenue download --reference 2026-05 --output ./downloads --force
cnpj-db-loader federal-revenue download 2026-05 --output ./downloads --force
```

Use the current calendar month when you want the command to fail if that month has not been published yet:

```bash
cnpj-db-loader federal-revenue check --current
```

If an explicit or current reference does not exist in the public share, the command fails before listing or downloading files and reports the latest available reference. Running without `--reference`, without `[reference]`, and without `--current` keeps the default behavior of selecting the latest published reference.

## Download behavior

`download` creates a child directory named with the selected reference inside the configured output root.

For example:

```bash
cnpj-db-loader federal-revenue download --output ./downloads --reference 2026-05 --force
```

writes files to:

```text
./downloads/2026-05
```

The downloader:

- lists `.zip` files from the selected monthly reference
- skips a completed local file when its size matches the remote size
- writes incomplete transfers as `<file>.part`
- retries each failed file before marking it as failed
- uses `--overwrite` only when a completed local file should be downloaded again

## Full sync

`sync` runs the remote download and then the local loader pipeline:

```bash
cnpj-db-loader federal-revenue sync \
  --output ./downloads \
  --db-url "postgresql://user:password@localhost:5432/cnpj" \
  --load-batch-size 500 \
  --materialize-batch-size 50000 \
  --verbose-progress \
  --force
```

Custom output directories can be used when an automation needs fixed paths:

```bash
cnpj-db-loader federal-revenue sync \
  --reference 2026-05 \
  --output ./downloads \
  --extract-output ./work/2026-05/extracted \
  --sanitize-output ./work/2026-05/sanitized \
  --force
```

## Options

| Option                                  | Applies to                  | Purpose                                                             |
| --------------------------------------- | --------------------------- | ------------------------------------------------------------------- |
| `[reference]` / `--reference <yyyy-mm>` | `check`, `download`, `sync` | Select a specific monthly reference.                                |
| `--current`                             | `check`, `download`, `sync` | Select the current calendar month.                                  |
| `--output <path>`                       | `download`, `sync`          | Download root directory. The reference folder is created inside it. |
| `--retries <number>`                    | `download`, `sync`          | Retry attempts per file. Defaults to 3.                             |
| `--overwrite`                           | `download`, `sync`          | Redownload files even when a completed local copy already exists.   |
| `--extract-output <path>`               | `sync`                      | Custom extraction output directory.                                 |
| `--sanitize-output <path>`              | `sync`                      | Custom sanitized output directory.                                  |
| `--db-url <url>`                        | `sync`                      | Override the saved PostgreSQL URL for the import phase.             |
| `--dataset <dataset>`                   | `sync`                      | Restrict the import phase to one dataset.                           |
| `--load-batch-size <size>`              | `sync`                      | Import load batch size.                                             |
| `--materialize-batch-size <size>`       | `sync`                      | Materialization chunk size.                                         |
| `--verbose-progress`                    | `sync`                      | Show detailed import progress.                                      |
| `--base-url <url>`                      | `check`, `download`, `sync` | Override the WebDAV base URL.                                       |
| `--share-token <token>`                 | `check`, `download`, `sync` | Override the public share token.                                    |
| `--force`                               | `download`, `sync`          | Skip confirmation prompts.                                          |

## Notes

This module intentionally does not include Redis, workers, or schedulers. For phase 1, the CLI remains responsible for the deterministic one-shot workflow. A future external runner can call these commands or import the public service functions and add queueing, locks, retries across jobs, and operating-system scheduling without making the core loader heavier.
