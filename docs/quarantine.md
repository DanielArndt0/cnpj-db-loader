# Quarentena

O serviço `quarantine` é uma superfície de CLI somente leitura para inspecionar as linhas gravadas na tabela `quarentena_importacao` durante a importação.

Tanto o fluxo de importação padrão orientado por Node quanto o fluxo de importação direta híbrida no PostgreSQL reutilizam essa mesma tabela. O fluxo híbrido grava na quarentena as inconsistências conhecidas de validação em nível de linha antes de inserir as linhas válidas nas tabelas de staging.

## Comandos

| Comando                | Objetivo                                                         |
| ---------------------- | ---------------------------------------------------------------- |
| `quarantine stats`     | Mostra totais e contagens agrupadas das linhas de quarentena.    |
| `quarantine list`      | Lista as linhas em quarentena com filtros e paginação opcionais. |
| `quarantine show <id>` | Mostra em detalhe uma linha em quarentena.                       |

## Filtros suportados

| Opção               | Comandos                | Descrição                                                         |
| ------------------- | ----------------------- | ----------------------------------------------------------------- |
| `--db-url <url>`    | `stats`, `list`, `show` | Sobrescreve a URL PostgreSQL persistida.                          |
| `--dataset <name>`  | `stats`, `list`         | Filtra as linhas pelo nome do dataset.                            |
| `--category <name>` | `stats`, `list`         | Filtra as linhas pela categoria de erro.                          |
| `--stage <name>`    | `stats`, `list`         | Filtra as linhas pela etapa de erro.                              |
| `--retryable`       | `stats`, `list`         | Mantém apenas as linhas marcadas como reprocessáveis.             |
| `--terminal`        | `stats`, `list`         | Mantém apenas as linhas marcadas como terminais.                  |
| `--limit <number>`  | `list`                  | Limita o número de linhas retornadas. Padrão: `20`.               |
| `--after-id <id>`   | `list`                  | Retorna as linhas estritamente após o id de quarentena informado. |

## Exemplos

```bash
cnpj-db-loader quarantine stats
cnpj-db-loader quarantine stats --dataset establishments --category invalid_utf8_sequence --retryable
cnpj-db-loader quarantine list --dataset establishments --limit 10
cnpj-db-loader quarantine list --terminal --after-id 500
cnpj-db-loader quarantine show 42
```

## Notas

- `quarantine` é intencionalmente somente leitura. Não reprocessa nem altera as linhas em quarentena.
- O serviço garante automaticamente que a tabela `quarentena_importacao` e suas colunas mais recentes existam antes de consultar.
- Um futuro comando de replay/recuperação pode reutilizar os mesmos filtros para atingir linhas reprocessáveis ou terminais.
- As linhas de validação híbrida do PostgreSQL usam a etapa `postgres_direct_staging_validation`.
- O modo híbrido mantém o schema de quarentena existente. Não cria uma segunda tabela de quarentena.
- Para as linhas de validação híbrida do lado do SQL, `linha_bruta` contém a representação em texto JSON da linha bruta temporária e `deslocamento_checkpoint` pode ser `NULL` quando o deslocamento exato de bytes da origem não está disponível após o `\copy`.
- Falhas estruturais de CSV ou falhas de baixo nível no `\copy` ainda interrompem a fase antes que a quarentena de linha do lado do SQL possa rodar; execute `validate` e `sanitize` primeiro.
