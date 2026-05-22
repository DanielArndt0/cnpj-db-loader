# Federal Revenue integration

The `federal-revenue` command group automates the remote monthly CNPJ dataset phase for the Brazilian Federal Revenue public share.

This feature is additive. It does not replace the stable local commands. The full `sync` command uses the same internal services already used by the manual flow:

```text
check/download -> extract -> validate -> sanitize -> import
```

The command also has the shorter alias `revenue`.

## Commands

```bash
cnpj-db-loader federal-revenue check
cnpj-db-loader federal-revenue download --output ./downloads --force
cnpj-db-loader federal-revenue status --output ./downloads
cnpj-db-loader federal-revenue retry --output ./downloads --force
cnpj-db-loader federal-revenue clean --output ./downloads --partials --force
cnpj-db-loader federal-revenue sync --output ./downloads --db-url "postgresql://user:password@localhost:5432/cnpj" --force
```

Alias examples:

```bash
cnpj-db-loader revenue check
cnpj-db-loader revenue status 2026-05 --output ./downloads
cnpj-db-loader revenue retry 2026-05 --output ./downloads --force
```

## Reference selection

By default, remote commands list the public share and select the latest available folder in the `YYYY-MM` format.

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

If an explicit or current reference does not exist in the public share, the command fails before listing or downloading files and reports the latest available reference:

```text
VALIDATION_ERROR Federal Revenue reference not found: 2026-06. Latest available reference is 2026-05.
```

Running without `--reference`, without `[reference]`, and without `--current` keeps the default behavior of selecting the latest published reference.

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
- validates local size against the remote WebDAV size when available
- writes a local manifest for status, retry, clean, and future runner automation
- uses `--overwrite` only when a completed local file should be downloaded again

## Local manifest

Each downloaded reference receives a local operational manifest:

```text
<output>/<reference>/.cnpj-db-loader/federal-revenue/manifest.json
```

The manifest tracks:

- selected reference
- remote base URL
- output path
- file names and paths
- remote size and local size
- local status: `downloaded`, `failed`, `partial`, or `missing`
- last command and last status
- error message when a file fails

This state is intentionally local and file-based. It does not require Redis or PostgreSQL.

## Status

Use `status` to inspect the local reference state without starting a new download:

```bash
cnpj-db-loader federal-revenue status 2026-05 --output ./downloads
```

The command exits with code `0` when the local reference is complete. It exits with code `1` when the manifest is missing or when at least one file is failed, partial, or missing.

## Retry

Use `retry` to download only incomplete files tracked by the manifest:

```bash
cnpj-db-loader federal-revenue retry 2026-05 --output ./downloads --force
```

Completed files are kept. Failed, partial, and missing files are retried according to `--retries`.

## Clean

Use `clean` for local maintenance:

```bash
cnpj-db-loader federal-revenue clean 2026-05 --output ./downloads --partials --force
cnpj-db-loader federal-revenue clean 2026-05 --output ./downloads --failed --force
cnpj-db-loader federal-revenue clean 2026-05 --output ./downloads --all --force
```

Cleanup modes:

| Mode         | Behavior                                                                              |
| ------------ | ------------------------------------------------------------------------------------- |
| `--partials` | Removes only `.part` files.                                                           |
| `--failed`   | Removes failed and partial files tracked by the manifest, then marks them as missing. |
| `--all`      | Removes the entire local reference folder, including ZIP files and manifest state.    |

Only one cleanup mode can be used at a time.

## Sync lock

`sync` creates a local lock file before running the full pipeline:

```text
<output>/<reference>/.cnpj-db-loader/federal-revenue/sync.lock
```

This prevents two sync processes from using the same reference folder at the same time.

If a previous process was interrupted and the lock is stale, use `--force-lock` only after confirming that no other sync is running:

```bash
cnpj-db-loader federal-revenue sync 2026-05 --output ./downloads --force-lock --force
```

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

## Exit codes

| Command    | Exit code `0`                                        | Exit code `1`                                                            |
| ---------- | ---------------------------------------------------- | ------------------------------------------------------------------------ |
| `check`    | Reference and file list were resolved.               | Invalid reference, missing remote reference, or WebDAV error.            |
| `download` | Download completed without failed files.             | One or more files failed.                                                |
| `status`   | Local manifest exists and every file is downloaded.  | Manifest missing or at least one file is failed/partial/missing.         |
| `retry`    | Retry finished with no failed/partial/missing files. | At least one file is still failed, partial, or missing.                  |
| `clean`    | Cleanup completed.                                   | Invalid cleanup mode or invalid reference.                               |
| `sync`     | Full pipeline completed.                             | Download, extraction, validation, sanitization, import, or lock failure. |

## Options

| Option                                  | Applies to                                              | Purpose                                                             |
| --------------------------------------- | ------------------------------------------------------- | ------------------------------------------------------------------- |
| `[reference]` / `--reference <yyyy-mm>` | `check`, `download`, `status`, `retry`, `clean`, `sync` | Select a specific monthly reference.                                |
| `--current`                             | `check`, `download`, `status`, `retry`, `clean`, `sync` | Select the current calendar month.                                  |
| `--output <path>`                       | `download`, `status`, `retry`, `clean`, `sync`          | Download root directory. The reference folder is created inside it. |
| `--retries <number>`                    | `download`, `retry`, `sync`                             | Retry attempts per file. Defaults to 3.                             |
| `--overwrite`                           | `download`, `retry`, `sync`                             | Redownload files even when a completed local copy already exists.   |
| `--partials`                            | `clean`                                                 | Remove only `.part` files.                                          |
| `--failed`                              | `clean`                                                 | Remove failed and partial files tracked by the manifest.            |
| `--all`                                 | `clean`                                                 | Remove the entire local reference folder.                           |
| `--force-lock`                          | `sync`                                                  | Remove an existing sync lock before starting.                       |
| `--extract-output <path>`               | `sync`                                                  | Custom extraction output directory.                                 |
| `--sanitize-output <path>`              | `sync`                                                  | Custom sanitized output directory.                                  |
| `--db-url <url>`                        | `sync`                                                  | Override the saved PostgreSQL URL for the import phase.             |
| `--dataset <dataset>`                   | `sync`                                                  | Restrict the import phase to one dataset.                           |
| `--load-batch-size <size>`              | `sync`                                                  | Import load batch size.                                             |
| `--materialize-batch-size <size>`       | `sync`                                                  | Materialization chunk size.                                         |
| `--verbose-progress`                    | `sync`                                                  | Show detailed import progress.                                      |
| `--base-url <url>`                      | `check`, `download`, `retry`, `sync`                    | Override the WebDAV base URL.                                       |
| `--share-token <token>`                 | `check`, `download`, `retry`, `sync`                    | Override the public share token.                                    |
| `--force`                               | `download`, `retry`, `clean`, `sync`                    | Skip confirmation prompts.                                          |

## Notes

This module intentionally does not include Redis, workers, or schedulers. The CLI remains responsible for deterministic one-shot operations and local operational control. A future external runner can call these commands or import the public service functions and add queueing, job-level retries, scheduling, and notifications without making the core loader heavier.
