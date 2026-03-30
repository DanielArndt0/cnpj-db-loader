# Commands reference

| Command                         | Purpose                                                                                                                                                           |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `inspect <input>`               | Detect whether the input is zipped, extracted, mixed, or empty.                                                                                                   |
| `extract <input>`               | Extract every ZIP archive found inside the input directory.                                                                                                       |
| `validate <input>`              | Validate an extracted dataset tree.                                                                                                                               |
| `sanitize <input>`              | Prepare a sanitized dataset tree before import.                                                                                                                   |
| `schema print`                  | Print a generated PostgreSQL schema profile (`full`, `final`, or `staging`) to stdout.                                                                            |
| `schema generate`               | Write a generated schema profile to the current working directory by default.                                                                                     |
| `database config set <url>`     | Persist the default PostgreSQL URL.                                                                                                                               |
| `database config show`          | Show the saved PostgreSQL URL.                                                                                                                                    |
| `database config test`          | Test the connection using the saved or overridden URL.                                                                                                            |
| `database config reset`         | Remove the saved PostgreSQL URL after confirmation.                                                                                                               |
| `database cleanup staging`      | Truncate staging tables and optionally clear linked materialization checkpoints for a validated path.                                                             |
| `database cleanup materialized` | Truncate final relational tables populated by materialization in dependency-safe order.                                                                           |
| `database cleanup checkpoints`  | Clear load checkpoints, materialization checkpoints, or both without truncating staging or final tables.                                                          |
| `database cleanup plans`        | Delete saved import plans. Related plan files and materialization checkpoints are removed by database cascade.                                                    |
| `import <input>`                | Run the full pipeline: plan, load validated files into staging/direct final targets, materialize staged datasets into final tables, and finalize the import plan. |
| `import load <input>`           | Prepare the plan and run only the load phase. Heavy datasets stop in `staging_*`; domain datasets still upsert directly into the final schema.                    |
| `import materialize <input>`    | Resume from the saved import plan and materialize staged datasets into the final relational tables with resumable chunks.                                         |
| `doctor`                        | Run a quick environment diagnosis.                                                                                                                                |
| `quarantine stats`              | Show aggregate counts for the `import_quarantine` table.                                                                                                          |
| `quarantine list`               | List quarantined rows with optional filters.                                                                                                                      |
| `quarantine show`               | Show one quarantined row in detail.                                                                                                                               |

## Examples

```bash
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
