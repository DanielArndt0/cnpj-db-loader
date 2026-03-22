# CLI

## Public command surface

```bash
cnpj-db-loader inspect <input>
cnpj-db-loader extract <input> [--output <path>]
cnpj-db-loader validate <input>
cnpj-db-loader schema print
cnpj-db-loader schema generate [--name <name>] [--output <path>]
cnpj-db-loader db set <url>
cnpj-db-loader db show
cnpj-db-loader db test [--db-url <url>]
cnpj-db-loader db reset [--yes]
cnpj-db-loader doctor [--input <path>] [--db-url <url>]
```

## Design notes

- The public CLI stays intentionally small.
- Placeholder commands are not exposed.
- Positional arguments are preferred when they make commands easier to type.
- Only destructive actions ask for confirmation.
