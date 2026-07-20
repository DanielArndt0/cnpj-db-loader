# CLI

## Superfície pública de comandos

```bash
cnpj-db-loader rfb check [reference] [--reference <yyyy-mm>] [--current]
cnpj-db-loader rfb download [reference] [--reference <yyyy-mm>] [--current] [--output <path>] [--retries <number>] [--overwrite] [-f]
cnpj-db-loader rfb status [reference] [--reference <yyyy-mm>] [--current] [--output <path>]
cnpj-db-loader rfb retry [reference] [--reference <yyyy-mm>] [--current] [--output <path>] [--retries <number>] [--overwrite] [-f]
cnpj-db-loader rfb clean [reference] [--reference <yyyy-mm>] [--current] [--output <path>] [--partials | --failed | --all] [-f]
cnpj-db-loader rfb sync [reference] [--reference <yyyy-mm>] [--current] [--output <path>] [--extract-output <path>] [--sanitize-output <path>] [--db-url <url>] [--dataset <name>] [--load-batch-size <size>] [--materialize-batch-size <size>] [--verbose-progress] [--force-lock] [-f]
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

## Notas de design

- A CLI pública é mantida intencionalmente pequena, mas o fluxo de importação agora expõe fases divididas para automação.
- `import` executa o pipeline inteiro, enquanto `import load` e `import materialize` mantêm o staging e a consolidação final executáveis de forma independente.
- Comandos de placeholder não são expostos.
- Argumentos posicionais são preferidos quando tornam os comandos mais fáceis de digitar.
- As ações destrutivas de manutenção do banco pedem confirmação, a menos que `--force` seja informado.

- `rfb` (alias `revenue`; o antigo `federal-revenue` continua como alias depreciado) é aditivo: automatiza apenas a fase de download mensal remoto de CNPJ e depois reutiliza os serviços existentes de extract, validate, sanitize e import.
- Os downloads da Receita Federal mantêm os arquivos completos por padrão e gravam as transferências incompletas como arquivos `.part` até o arquivo ser totalmente validado.
- `status`, `retry` e `clean` usam o manifesto local de referência para que um futuro runner externo possa inspecionar e retomar o fluxo sem duplicar as regras do loader.
- `sync` cria um arquivo de lock local para impedir que duas operações de sync completo usem a mesma pasta de referência ao mesmo tempo.
