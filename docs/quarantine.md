# Quarantine

The `quarantine` service is a read-only CLI surface for inspecting rows written to the `import_quarantine` table during import.

Both the standard Node-driven import flow and the hybrid PostgreSQL direct import flow reuse this same table. The hybrid flow writes known row-level validation inconsistencies to quarantine before inserting valid rows into staging tables.

## Commands

| Command                | Purpose                                                 |
| ---------------------- | ------------------------------------------------------- |
| `quarantine stats`     | Show totals and grouped counts for quarantine rows.     |
| `quarantine list`      | List quarantined rows with optional filters and paging. |
| `quarantine show <id>` | Show one quarantined row in detail.                     |

## Supported filters

| Option              | Commands                | Description                                            |
| ------------------- | ----------------------- | ------------------------------------------------------ |
| `--db-url <url>`    | `stats`, `list`, `show` | Override the persisted PostgreSQL URL.                 |
| `--dataset <name>`  | `stats`, `list`         | Filter rows by dataset name.                           |
| `--category <name>` | `stats`, `list`         | Filter rows by error category.                         |
| `--stage <name>`    | `stats`, `list`         | Filter rows by error stage.                            |
| `--retryable`       | `stats`, `list`         | Keep only rows marked as retryable.                    |
| `--terminal`        | `stats`, `list`         | Keep only rows marked as terminal.                     |
| `--limit <number>`  | `list`                  | Limit the number of returned rows. Defaults to `20`.   |
| `--after-id <id>`   | `list`                  | Return rows strictly after the provided quarantine id. |

## Examples

```bash
cnpj-db-loader quarantine stats
cnpj-db-loader quarantine stats --dataset establishments --category invalid_utf8_sequence --retryable
cnpj-db-loader quarantine list --dataset establishments --limit 10
cnpj-db-loader quarantine list --terminal --after-id 500
cnpj-db-loader quarantine show 42
```

## Notes

- `quarantine` is intentionally read-only. It does not retry or mutate quarantined rows.
- The service automatically ensures that the `import_quarantine` table and its newer columns exist before querying.
- A future replay/recovery command can reuse the same filters to target retryable or terminal rows.
- Hybrid PostgreSQL validation rows use the `postgres_direct_staging_validation` stage.
- Hybrid mode keeps the existing quarantine schema. It does not create a second quarantine table.
- For SQL-side hybrid validation rows, `raw_line` contains the JSON text representation of the temporary raw row and `checkpoint_offset` can be `NULL` when the exact source byte offset is unavailable after `\copy`.
- Structural CSV or low-level `\copy` failures still stop the phase before SQL-side row quarantine can run; execute `validate` and `sanitize` first.
