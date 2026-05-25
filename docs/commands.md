# Commands reference

| Command                         | Purpose                                                                                                                                                           |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `federal-revenue check`         | Check the selected or latest Federal Revenue monthly CNPJ reference and list the remote ZIP files.                                                                |
| `federal-revenue download`      | Download the selected Federal Revenue monthly CNPJ ZIP files with retries, `.part` files, and skip-on-existing behavior.                                          |
| `federal-revenue status`        | Read the local Federal Revenue manifest and report downloaded, failed, partial, and missing files.                                                                |
| `federal-revenue retry`         | Retry incomplete Federal Revenue files without redownloading completed files.                                                                                     |
| `federal-revenue clean`         | Clean local Federal Revenue `.part` files, failed/partial files, or a whole reference folder.                                                                     |
| `federal-revenue sync`          | Download, extract, validate, sanitize, and import the selected monthly CNPJ reference using the existing loader pipeline with a local sync lock.                  |
| `inspect <input>`               | Detect whether the input is zipped, extracted, mixed, or empty.                                                                                                   |
| `extract <input>`               | Extract every ZIP archive found inside the input directory.                                                                                                       |
| `validate <input>`              | Validate an extracted dataset tree.                                                                                                                               |
| `sanitize <input>`              | Prepare a sanitized dataset tree before import.                                                                                                                   |
| `schema print`                  | Print a generated PostgreSQL schema profile (`full`, `final`, or `staging`) to stdout. The final profile is simplified for fast first-load materialization.       |
| `schema generate`               | Write a generated schema profile to the current working directory by default.                                                                                     |
| `database config set <url>`     | Persist the default PostgreSQL URL.                                                                                                                               |
| `database config show`          | Show the saved PostgreSQL URL.                                                                                                                                    |
| `database config test`          | Test the connection using the saved or overridden URL.                                                                                                            |
| `database config reset`         | Remove the saved PostgreSQL URL after confirmation.                                                                                                               |
| `database cleanup staging`      | Truncate staging tables and optionally clear linked materialization checkpoints for a validated path.                                                             |
| `database cleanup materialized` | Truncate simplified final relational tables populated by materialization, including establishment secondary CNAEs when available, in safe order.                  |
| `database cleanup checkpoints`  | Clear load checkpoints, materialization checkpoints, or both without truncating staging or final tables.                                                          |
| `database cleanup plans`        | Delete saved import plans. Related plan files and materialization checkpoints are removed by database cascade.                                                    |
| `postgres generate-script`      | Generate a direct `psql` import script that loads sanitized Receita files without rewriting them into new CSV files.                                              |
| `postgres export-csv`           | Convert sanitized Receita files into normalized PostgreSQL-ready CSV files and generate a direct `psql` import script for audit/debug workflows.                  |
| `import <input>`                | Run the full pipeline: plan, load validated files into staging/direct final targets, materialize staged datasets into final tables, and finalize the import plan. |
| `import load <input>`           | Prepare the plan and run only the load phase. Heavy datasets stop in `staging_*`; domain datasets still upsert directly into the final schema.                    |
| `import materialize <input>`    | Resume from the saved import plan and materialize staged datasets into the final relational tables with resumable chunks.                                         |
| `doctor`                        | Run a quick environment diagnosis.                                                                                                                                |
| `quarantine stats`              | Show aggregate counts for the `import_quarantine` table.                                                                                                          |
| `quarantine list`               | List quarantined rows with optional filters.                                                                                                                      |
| `quarantine show`               | Show one quarantined row in detail.                                                                                                                               |

## Examples

```bash
cnpj-db-loader federal-revenue check
cnpj-db-loader federal-revenue check 2026-05
cnpj-db-loader federal-revenue download --output ./downloads --force
cnpj-db-loader federal-revenue sync --output ./downloads --db-url "postgresql://user:password@localhost:5432/cnpj" --force
cnpj-db-loader inspect ./downloads
cnpj-db-loader extract ./downloads
cnpj-db-loader validate ./downloads/extracted
cnpj-db-loader sanitize ./downloads/extracted
cnpj-db-loader schema generate --profile full --name receita-v2 --output ./artifacts/sql
cnpj-db-loader schema generate --profile staging
cnpj-db-loader schema print --profile final
cnpj-db-loader database config set "postgresql://user:password@localhost:5432/cnpj"
cnpj-db-loader database config test
cnpj-db-loader database cleanup staging --validated-path ./downloads/sanitized --force
cnpj-db-loader database cleanup materialized --dataset companies --force
cnpj-db-loader database cleanup checkpoints --phase materialization --validated-path ./downloads/sanitized --force
cnpj-db-loader database cleanup plans --validated-path ./downloads/sanitized --force
cnpj-db-loader postgres generate-script ./downloads/sanitized --output ./downloads/postgres-direct --force
psql "postgres://user:password@localhost:5432/cnpj" -f ./downloads/postgres-direct/import-postgres-direct.sql
cnpj-db-loader import ./downloads/sanitized
cnpj-db-loader import ./downloads/sanitized --db-url "postgresql://user:password@localhost:5432/cnpj"
cnpj-db-loader import ./downloads/sanitized --dataset companies --load-batch-size 500
cnpj-db-loader import load ./downloads/sanitized --load-batch-size 20000
cnpj-db-loader import materialize ./downloads/sanitized --materialize-batch-size 50000
cnpj-db-loader database cleanup staging --validated-path ./downloads/sanitized
cnpj-db-loader import ./downloads/sanitized --force
cnpj-db-loader quarantine stats
cnpj-db-loader quarantine stats --dataset establishments --category invalid_utf8_sequence --retryable
cnpj-db-loader quarantine list --dataset establishments --limit 10
cnpj-db-loader quarantine list --terminal --after-id 500
cnpj-db-loader quarantine show 42
```

## PostgreSQL direct import helper

```bash
cnpj-db-loader postgres generate-script <input> [--output <path>] [--dataset <dataset>] [--script-name <name>] [--source-encoding <encoding>] [-f]
cnpj-db-loader postgres export-csv <input> [--output <path>] [--dataset <dataset>] [--script-name <name>] [-f]
```

`postgres generate-script` is the recommended hybrid workflow. The loader performs extraction, validation and sanitization, then generates a `psql` script that loads the sanitized Receita files directly through `\copy`.

`postgres export-csv` remains available when you explicitly want a normalized CSV output tree for audit/debug purposes.

Options:

- `--output <path>`: directory where manifest and SQL script are generated.
- `--dataset <dataset>`: generate only one dataset block.
- `--script-name <name>`: custom generated SQL script name.
- `--source-encoding <encoding>`: source file encoding for `psql` copy operations. Defaults to `UTF8`.
- `-f, --force`: skip confirmation.
