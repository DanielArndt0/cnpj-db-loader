# Commands reference

| Command            | Purpose                                                                                                 |
| ------------------ | ------------------------------------------------------------------------------------------------------- |
| `inspect <input>`  | Detect whether the input is zipped, extracted, mixed, or empty.                                         |
| `extract <input>`  | Extract every ZIP archive found inside the input directory.                                             |
| `validate <input>` | Validate an extracted dataset tree.                                                                     |
| `sanitize <input>` | Prepare a sanitized dataset tree before import.                                                         |
| `schema print`     | Print a generated PostgreSQL schema profile (`full`, `final`, or `staging`) to stdout.                  |
| `schema generate`  | Write a generated schema profile to the current working directory by default.                           |
| `db set <url>`     | Persist the default PostgreSQL URL.                                                                     |
| `db show`          | Show the saved PostgreSQL URL.                                                                          |
| `db test`          | Test the connection using the saved or overridden URL.                                                  |
| `db reset`         | Remove the saved PostgreSQL URL.                                                                        |
| `import <input>`   | Import validated dataset files into PostgreSQL using streaming batches and conflict-safe deduplication. |
| `doctor`           | Run a quick environment diagnosis.                                                                      |
| `quarantine stats` | Show aggregate counts for the `import_quarantine` table.                                                |
| `quarantine list`  | List quarantined rows with optional filters.                                                            |
| `quarantine show`  | Show one quarantined row in detail.                                                                     |

## Examples

```bash
cnpj-db-loader inspect ./downloads
cnpj-db-loader extract ./downloads
cnpj-db-loader validate ./downloads/extracted
cnpj-db-loader sanitize ./downloads/extracted
cnpj-db-loader schema generate --profile full --name receita-v2 --output ./artifacts/sql
cnpj-db-loader schema generate --profile staging
cnpj-db-loader schema print --profile final
cnpj-db-loader db set "postgresql://user:password@localhost:5432/cnpj"
cnpj-db-loader db test
cnpj-db-loader import ./downloads/sanitized
cnpj-db-loader import ./downloads/sanitized --db-url "postgresql://user:password@localhost:5432/cnpj"
cnpj-db-loader import ./downloads/sanitized --dataset companies --batch-size 500
cnpj-db-loader import ./downloads/sanitized --force
cnpj-db-loader quarantine stats
cnpj-db-loader quarantine stats --dataset establishments --category invalid_utf8_sequence --retryable
cnpj-db-loader quarantine list --dataset establishments --limit 10
cnpj-db-loader quarantine list --terminal --after-id 500
cnpj-db-loader quarantine show 42
```
