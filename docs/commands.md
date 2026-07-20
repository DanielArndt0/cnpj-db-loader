# Referência de comandos

| Comando                         | Objetivo                                                                                                                                                                          |
| ------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `rfb config set`                | Persiste configurações WebDAV da Receita Federal, como share token, URL do WebDAV e user agent, no arquivo de config local.                                                       |
| `rfb config show`               | Mostra a configuração efetiva da Receita Federal.                                                                                                                                 |
| `rfb config test`               | Testa a conexão WebDAV configurada da Receita Federal.                                                                                                                            |
| `rfb config reset`              | Redefine uma ou todas as configurações persistidas da Receita Federal.                                                                                                            |
| `rfb check`                     | Verifica a referência mensal de CNPJ selecionada ou mais recente da Receita Federal e lista os arquivos ZIP remotos.                                                              |
| `rfb download`                  | Baixa os arquivos ZIP mensais de CNPJ da Receita Federal com repetições, arquivos `.part` e comportamento de ignorar já existentes.                                               |
| `rfb status`                    | Lê o manifesto local da Receita Federal e informa os arquivos baixados, falhos, parciais e ausentes.                                                                              |
| `rfb retry`                     | Repete os arquivos incompletos da Receita Federal sem rebaixar os arquivos já completos.                                                                                          |
| `rfb clean`                     | Limpa arquivos `.part`, arquivos falhos/parciais ou toda a pasta de uma referência local da Receita Federal.                                                                      |
| `rfb sync`                      | Baixa, extrai, valida, sanitiza e importa a referência mensal de CNPJ selecionada usando o pipeline existente do loader, com um lock de sync local.                               |
| `inspect <input>`               | Detecta se a entrada está compactada, extraída, mista ou vazia.                                                                                                                   |
| `extract <input>`               | Extrai arquivos ZIP, ZIP64 e ZIP divididos encontrados no diretório de entrada usando o motor 7-Zip embutido.                                                                     |
| `validate <input>`              | Valida uma árvore de dataset extraído.                                                                                                                                            |
| `sanitize <input>`              | Normaliza os arquivos de origem da Receita em saída UTF-8 validada antes da importação.                                                                                           |
| `schema print`                  | Imprime um perfil de schema PostgreSQL gerado (`full`, `final` ou `staging`) no stdout. O perfil final é simplificado para materialização rápida na primeira carga.               |
| `schema generate`               | Grava um perfil de schema gerado no diretório de trabalho atual por padrão.                                                                                                       |
| `database config set <url>`     | Persiste a URL PostgreSQL padrão.                                                                                                                                                 |
| `database config show`          | Mostra a URL PostgreSQL salva.                                                                                                                                                    |
| `database config test`          | Testa a conexão usando a URL salva ou sobrescrita.                                                                                                                                |
| `database config reset`         | Remove a URL PostgreSQL salva após confirmação.                                                                                                                                   |
| `database cleanup staging`      | Trunca as tabelas de staging e, opcionalmente, limpa os checkpoints de materialização vinculados a um caminho validado.                                                           |
| `database cleanup materialized` | Trunca as tabelas relacionais finais simplificadas populadas pela materialização, incluindo os CNAEs secundários dos estabelecimentos quando disponíveis, em ordem segura.        |
| `database cleanup checkpoints`  | Limpa checkpoints de carga, de materialização ou ambos, sem truncar as tabelas de staging ou finais.                                                                              |
| `database cleanup plans`        | Exclui os planos de importação salvos. Os arquivos de plano e os checkpoints de materialização relacionados são removidos por cascata do banco.                                   |
| `postgres generate-script`      | Gera um script de importação `psql` direto que carrega os arquivos sanitizados da Receita sem reescrevê-los em novos arquivos CSV.                                                |
| `postgres export-csv`           | Converte os arquivos sanitizados da Receita em arquivos CSV normalizados prontos para o PostgreSQL e gera um script de importação `psql` direto para fluxos de auditoria/debug.   |
| `import <input>`                | Executa o pipeline completo: planeja, carrega os arquivos validados nos alvos de staging/final diretos, materializa os datasets de staging nas tabelas finais e finaliza o plano. |
| `import load <input>`           | Prepara o plano e executa apenas a fase de carga. Datasets pesados param em `staging_*`; datasets de domínio ainda fazem upsert direto no schema final.                           |
| `import materialize <input>`    | Retoma do plano de importação salvo e materializa os datasets de staging nas tabelas relacionais finais em blocos retomáveis.                                                     |
| `doctor`                        | Executa um diagnóstico rápido do ambiente.                                                                                                                                        |
| `quarantine stats`              | Mostra contagens agregadas da tabela `quarentena_importacao`.                                                                                                                     |
| `quarantine list`               | Lista as linhas em quarentena com filtros opcionais.                                                                                                                              |
| `quarantine show`               | Mostra em detalhe uma linha em quarentena.                                                                                                                                        |

## Exemplos

```bash
cnpj-db-loader rfb config set share-token "<token-do-compartilhamento-publico>"
cnpj-db-loader rfb config test
cnpj-db-loader rfb check
cnpj-db-loader rfb check 2026-05
cnpj-db-loader rfb download --output ./downloads --force
cnpj-db-loader rfb sync --output ./downloads --db-url "postgresql://user:password@localhost:5432/cnpj" --force
cnpj-db-loader inspect ./downloads
cnpj-db-loader extract ./downloads
cnpj-db-loader validate ./downloads/extracted
cnpj-db-loader sanitize ./downloads/extracted
cnpj-db-loader schema generate --profile full --name receita-v3 --output ./artifacts/sql
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

## Auxiliar de importação direta no PostgreSQL

```bash
cnpj-db-loader postgres generate-script <input> [--output <path>] [--dataset <dataset>] [--script-name <name>] [--source-encoding <encoding>] [--transaction-mode <mode>] [--include <items>] [--skip-indexes] [--skip-analyze] [-f]
cnpj-db-loader postgres export-csv <input> [--output <path>] [--dataset <dataset>] [--script-name <name>] [-f]
```

`postgres generate-script` é o fluxo híbrido recomendado. O loader realiza extração, validação e sanitização e depois gera um script `psql` que carrega os arquivos sanitizados da Receita diretamente via `\copy`.

`postgres export-csv` continua disponível quando você explicitamente quer uma árvore de saída CSV normalizada para fins de auditoria/debug.

Opções:

- `--output <path>`: diretório onde o manifesto e o script SQL são gerados.
- `--dataset <dataset>`: gera apenas um bloco de dataset.
- `--script-name <name>`: nome personalizado do script SQL gerado.
- `--source-encoding <encoding>`: encoding dos arquivos de origem para as operações de copy do `psql`. Padrão: `UTF8`.
- `--transaction-mode <mode>`: estratégia de transação gerada: `single`, `phase` ou `none`. Padrão: `single`.
- `--include <items>`: alvos de geração separados por vírgula, como `domains,companies,establishments,secondary-cnaes,analyze`.
- `--skip-indexes`: pula a fase de índices gerada.
- `--skip-analyze`: pula a fase de analyze gerada.
- `-f, --force`: pula a confirmação.
