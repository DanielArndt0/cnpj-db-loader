# Commands reference

| Command            | Purpose                                                                                                 |
| ------------------ | ------------------------------------------------------------------------------------------------------- |
| `inspect <input>`  | Detect whether the input is zipped, extracted, mixed, or empty.                                         |
| `extract <input>`  | Extract every ZIP archive found inside the input directory.                                             |
| `validate <input>` | Validate an extracted dataset tree.                                                                     |
| `schema print`     | Print the generated PostgreSQL schema to stdout.                                                        |
| `schema generate`  | Write `schema.sql` to the current working directory by default.                                         |
| `db set <url>`     | Persist the default PostgreSQL URL.                                                                     |
| `db show`          | Show the saved PostgreSQL URL.                                                                          |
| `db test`          | Test the connection using the saved or overridden URL.                                                  |
| `db reset`         | Remove the saved PostgreSQL URL.                                                                        |
| `import <input>`   | Import validated dataset files into PostgreSQL using streaming batches and conflict-safe deduplication. |
| `doctor`           | Run a quick environment diagnosis.                                                                      |

## Examples

```bash
cnpj-db-loader inspect ./downloads
cnpj-db-loader extract ./downloads
cnpj-db-loader validate ./downloads/extracted
cnpj-db-loader schema generate --name receita-v2 --output ./artifacts/sql
cnpj-db-loader db set "postgresql://user:password@localhost:5432/cnpj"
cnpj-db-loader db test
cnpj-db-loader import ./downloads/extracted
cnpj-db-loader import ./downloads/extracted --db-url "postgresql://user:password@localhost:5432/cnpj"
cnpj-db-loader import ./downloads/extracted --dataset companies --batch-size 500
cnpj-db-loader import ./downloads/extracted --force
```
