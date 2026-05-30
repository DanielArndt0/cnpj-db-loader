# CLI

## Public command surface

```bash
cnpj-db-loader federal-revenue check [reference] [--reference <yyyy-mm>] [--current]
cnpj-db-loader federal-revenue download [reference] [--reference <yyyy-mm>] [--current] [--output <path>] [--retries <number>] [--overwrite] [-f]
cnpj-db-loader federal-revenue status [reference] [--reference <yyyy-mm>] [--current] [--output <path>]
cnpj-db-loader federal-revenue retry [reference] [--reference <yyyy-mm>] [--current] [--output <path>] [--retries <number>] [--overwrite] [-f]
cnpj-db-loader federal-revenue clean [reference] [--reference <yyyy-mm>] [--current] [--output <path>] [--partials | --failed | --all] [-f]
cnpj-db-loader federal-revenue sync [reference] [--reference <yyyy-mm>] [--current] [--output <path>] [--extract-output <path>] [--sanitize-output <path>] [--db-url <url>] [--dataset <name>] [--load-batch-size <size>] [--materialize-batch-size <size>] [--verbose-progress] [--force-lock] [-f]
cnpj-db-loader inspect <input>
cnpj-db-loader extract <input> [--output <path>]
cnpj-db-loader validate <input>
cnpj-db-loader sanitize <input> [--output <path>] [--dataset <name>] [--source-encoding <encoding>] [--allow-replacement-chars] [-f]
cnpj-db-loader schema print [--profile <profile>]
cnpj-db-loader schema generate [--name <name>] [--output <path>] [--profile <profile>]
cnpj-db-loader database config set <url>
cnpj-db-loader database config show
cnpj-db-loader database config test [--db-url <url>]
cnpj-db-loader database config reset [--force]
cnpj-db-loader database cleanup staging [--db-url <url>] [--dataset <name>] [--validated-path <path>] [--force]
cnpj-db-loader database cleanup materialized [--db-url <url>] [--dataset <name>] [--force]
cnpj-db-loader database cleanup checkpoints [--db-url <url>] [--phase <phase>] [--dataset <name>] [--validated-path <path>] [--plan-id <id>] [--force]
cnpj-db-loader database cleanup plans [--db-url <url>] [--validated-path <path>] [--plan-id <id>] [--force]
cnpj-db-loader import <input> [--db-url <url>] [--dataset <name>] [--load-batch-size <size>] [--materialize-batch-size <size>] [--verbose-progress] [-f]
cnpj-db-loader import load <input> [--db-url <url>] [--dataset <name>] [--load-batch-size <size>] [--verbose-progress] [-f]
cnpj-db-loader import materialize <input> [--db-url <url>] [--dataset <name>] [--materialize-batch-size <size>] [--verbose-progress] [-f]
cnpj-db-loader doctor [--input <path>] [--db-url <url>]
cnpj-db-loader quarantine stats [--dataset <name>] [--category <name>] [--stage <name>] [--retryable] [--terminal]
cnpj-db-loader quarantine list [--dataset <name>] [--category <name>] [--stage <name>] [--retryable] [--terminal] [--limit <number>] [--after-id <id>]
cnpj-db-loader quarantine show <id> [--db-url <url>]
```

## Design notes

- The public CLI stays intentionally small, but the import workflow now exposes split phases for automation.
- `import` runs the whole pipeline, while `import load` and `import materialize` keep staging and final consolidation independently runnable.
- Placeholder commands are not exposed.
- Positional arguments are preferred when they make commands easier to type.
- Destructive database maintenance actions ask for confirmation unless `--force` is provided.

- `federal-revenue` (alias `revenue`) is additive: it only automates the remote monthly CNPJ download phase and then reuses the existing extract, validate, sanitize, and import services.
- Federal Revenue downloads keep completed files by default and write incomplete transfers as `.part` files until the file is fully validated.
- `status`, `retry`, and `clean` use the local reference manifest so a future external runner can inspect and resume the workflow without duplicating loader rules.
- `sync` creates a local lock file to prevent two full sync operations from using the same reference folder at the same time.
