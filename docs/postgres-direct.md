# Fluxo de importação direta no PostgreSQL

O fluxo de importação direta no PostgreSQL é um caminho híbrido para ambientes em que o importador retomável padrão é caro demais para uma carga mensal completa.

Ele mantém os passos seguros de preparação dentro do CNPJ DB Loader e move o trabalho mais pesado de carga/materialização do banco para scripts `psql` gerados.

## Fluxo pretendido

```bash
cnpj-db-loader rfb download --output ./downloads
cnpj-db-loader extract ./downloads/<referencia>
cnpj-db-loader validate ./downloads/<referencia>/extracted
cnpj-db-loader sanitize ./downloads/<referencia>/extracted
cnpj-db-loader postgres generate-script ./downloads/<referencia>/sanitized --output ./downloads/<referencia>/postgres-direct --source-encoding UTF8 --transaction-mode phase --force
psql -d "postgres://postgres:postgres@localhost:5432/cnpj" -f ./downloads/<referencia>/postgres-direct/import-postgres-direct.sql
```

O loader continua responsável por:

- download da Receita Federal e controle do manifesto local
- extração
- validação
- sanitização
- preservar os arquivos sanitizados da Receita sem reescrever o dataset inteiro
- gerar os scripts `psql` de importação modulares
- opcionalmente exportar arquivos CSV prontos para o PostgreSQL via `postgres export-csv` quando uma árvore de CSV para auditoria/debug for útil

O PostgreSQL fica então responsável por:

- carregar os arquivos sanitizados da Receita em tabelas brutas temporárias via `\copy`
- conversão do lado do SQL de datas, valores numéricos e campos anuláveis
- colocar em quarentena as inconsistências conhecidas de nível de linha na tabela `quarentena_importacao` existente
- atualizar as tabelas `planos_importacao`, `arquivos_plano_importacao`, `checkpoints_importacao` e `checkpoints_materializacao` existentes
- popular as tabelas de staging usando apenas linhas válidas
- upserts baseados em conjuntos nas tabelas finais
- materialização de `estabelecimento_cnaes_secundarios`
- atualização das estatísticas do planejador via `ANALYZE`

## Comando

```bash
cnpj-db-loader postgres generate-script <input> [--output <path>] [--dataset <dataset>] [--script-name <name>] [--source-encoding <encoding>] [--transaction-mode <mode>] [--include <items>] [--skip-indexes] [--skip-analyze] [-f]
```

### Argumentos

| Argumento | Descrição                                   |
| --------- | ------------------------------------------- |
| `<input>` | Caminho do diretório de dataset sanitizado. |

### Opções

| Opção                          | Descrição                                                                                                                                                                                                                                                         |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--output <path>`              | Diretório de saída personalizado para os scripts SQL gerados e o manifesto.                                                                                                                                                                                       |
| `--dataset <dataset>`          | Gera scripts apenas para um bloco de dataset. Útil para debug.                                                                                                                                                                                                    |
| `--script-name <name>`         | Nome do script orquestrador gerado. Padrão: `import-postgres-direct.sql`.                                                                                                                                                                                         |
| `--source-encoding <encoding>` | Encoding dos arquivos de origem usado pelo `psql` ao ler os arquivos sanitizados da Receita. Padrão: `UTF8`, porque o comando `sanitize` atual grava saída UTF-8. Use `WIN1252` ou `LATIN1` apenas para arquivos sanitizados legados gerados por versões antigas. |
| `--transaction-mode <mode>`    | Estratégia de transação dos scripts gerados: `single`, `phase` ou `none`. Padrão: `single`.                                                                                                                                                                       |
| `--include <items>`            | Etapas a incluir, separadas por vírgula: `domains`, `companies`, `establishments`, `partners`, `simples`, `secondary-cnaes`, `indexes`, `analyze`.                                                                                                                |
| `--skip-indexes`               | Não gera a etapa `indexes.sql`.                                                                                                                                                                                                                                   |
| `--skip-analyze`               | Não gera a etapa `analyze.sql`.                                                                                                                                                                                                                                   |
| `-f, --force`                  | Pula a confirmação interativa.                                                                                                                                                                                                                                    |

## Modos de transação

### `single`

O orquestrador envolve todas as etapas incluídas em uma única transação:

```text
BEGIN
  setup
  carrega domínios
  carrega empresas
  carrega estabelecimentos
  carrega sócios
  carrega simples
  materializa
  materializa CNAEs secundários
  índices
  analyze
COMMIT
```

Este é o modo mais seguro, porque uma falha faz rollback de toda a execução, mas também é o menos conveniente para importações muito grandes, porque uma falha tardia exige recomeçar do zero.

### `phase`

Cada script de fase gerado envolve o seu próprio trabalho em uma transação.

Este é o modo recomendado para execuções locais longas, porque as fases concluídas permanecem confirmadas se uma fase posterior falhar.

### `none`

Nenhum wrapper de transação gerado é adicionado.

Este modo é útil para cenários agressivos de benchmark, mas pode deixar dados parciais se um comando falhar.

## Estrutura de saída

O comando cria um diretório de saída direto do PostgreSQL modular:

```text
postgres-direct/
  manifest.json
  import-postgres-direct.sql
  setup.sql
  load-domains.sql
  load-companies.sql
  load-establishments.sql
  load-partners.sql
  load-simples.sql
  materialize.sql
  materialize-secondary-cnaes.sql
  indexes.sql
  analyze.sql
```

O arquivo `import-postgres-direct.sql` é um orquestrador que executa os scripts de fase incluídos na ordem correta com `\ir`.

Você pode executar o fluxo completo:

```bash
psql -d "postgres://postgres:postgres@localhost:5432/cnpj" -f ./postgres-direct/import-postgres-direct.sql
```

Ou executar scripts de fase individuais:

```bash
psql -d "postgres://postgres:postgres@localhost:5432/cnpj" -f ./postgres-direct/load-domains.sql
psql -d "postgres://postgres:postgres@localhost:5432/cnpj" -f ./postgres-direct/load-establishments.sql
```

## Geração parcial

Gerar apenas os scripts de domínio:

```bash
cnpj-db-loader postgres generate-script ./downloads/<referencia>/sanitized --output ./downloads/<referencia>/postgres-direct --include domains --transaction-mode phase --force
```

Gerar sem índices e analyze:

```bash
cnpj-db-loader postgres generate-script ./downloads/<referencia>/sanitized --output ./downloads/<referencia>/postgres-direct --skip-indexes --skip-analyze --force
```

Gerar apenas estabelecimentos e CNAEs secundários:

```bash
cnpj-db-loader postgres generate-script ./downloads/<referencia>/sanitized --output ./downloads/<referencia>/postgres-direct --include establishments,secondary-cnaes,analyze --transaction-mode phase --force
```

## Comportamento dos scripts gerados

Os scripts gerados:

1. habilitam `ON_ERROR_STOP` para o `psql`;
2. definem o client encoding configurado para as operações de copy do `psql`;
3. registram a execução híbrida usando as tabelas de plano de importação existentes;
4. carregam cada arquivo sanitizado da Receita em uma tabela de texto bruto temporária;
5. validam as inconsistências conhecidas de nível de linha antes da inserção no staging;
6. gravam as linhas inválidas na tabela `quarentena_importacao` existente em vez de descartá-las silenciosamente;
7. atualizam os checkpoints de arquivo existentes após cada arquivo de origem concluído;
8. convertem os valores válidos dentro do PostgreSQL e os inserem em `staging_empresas`, `staging_estabelecimentos`, `staging_socios` e `staging_simples`;
9. materializam as tabelas finais `empresas`, `estabelecimentos`, `socios` e `simples` usando SQL baseado em conjuntos, atualizando os checkpoints de materialização existentes;
10. populam `estabelecimento_cnaes_secundarios` a partir de `cnae_fiscal_secundaria_raw`;
11. opcionalmente geram uma fase de índices;
12. opcionalmente executam `ANALYZE` nas tabelas afetadas.

Os scripts não recriam o schema. Rode o schema normal primeiro:

```bash
cnpj-db-loader schema generate --profile full --output ./sql/schema.sql
psql -d "postgres://postgres:postgres@localhost:5432/cnpj" -f ./sql/schema.sql
```

## Compatibilidade de quarentena e checkpoint

O modo híbrido reutiliza as mesmas tabelas operacionais já criadas pelo perfil de schema `full`:

```text
planos_importacao
arquivos_plano_importacao
checkpoints_importacao
checkpoints_materializacao
quarentena_importacao
```

Inconsistências conhecidas de nível de linha, como valores obrigatórios ausentes ou valores numéricos/de data transformados inválidos, são gravadas em `quarentena_importacao`. As linhas válidas seguem para o staging na mesma execução.

Como o caminho SQL direto valida as linhas depois que o `\copy` as carregou em uma tabela bruta temporária, as colunas de quarentena existentes permanecem compatíveis, mas alguns detalhes de nível de origem são representados de forma diferente: `linha_bruta` armazena a representação em texto JSON da linha bruta carregada e `deslocamento_checkpoint` fica `NULL` quando o deslocamento de bytes original exato não está disponível.

Os scripts diretos intencionalmente evitam criar um segundo modelo de quarentena ou novas tabelas operacionais permanentes. O SQL gerado apenas reutiliza o schema existente do loader.

### Limitação importante

Estrutura de CSV malformada ou falhas de baixo nível no `\copy` ainda interrompem a fase atual, porque o PostgreSQL rejeita o stream de origem antes que a validação SQL de nível de linha possa rodar. Os passos normais `validate` e `sanitize` devem rodar antes de `postgres generate-script` para que problemas estruturais e de encoding sejam tratados mais cedo no pipeline.

Ao executar scripts de fase individuais manualmente, execute `setup.sql` primeiro para que as tabelas de plano de importação existentes sejam inicializadas para a execução híbrida.

## Monitorando o PostgreSQL enquanto a importação roda

O modo híbrido mantém os checkpoints do loader leves intencionalmente. Use as views nativas do PostgreSQL para monitorar o trabalho pesado.

### Queries ativas

```sql
SELECT
  pid,
  now() - query_start AS duration,
  state,
  wait_event_type,
  wait_event,
  left(query, 200) AS query
FROM pg_stat_activity
WHERE datname = current_database()
  AND state <> 'idle'
ORDER BY query_start;
```

### Progresso do COPY

```sql
SELECT
  pid,
  command,
  type,
  bytes_processed,
  bytes_total,
  tuples_processed,
  CASE
    WHEN bytes_total > 0
    THEN round((bytes_processed::numeric / bytes_total::numeric) * 100, 2)
    ELSE NULL
  END AS percent
FROM pg_stat_progress_copy;
```

Com auto-refresh no `psql`:

```sql
SELECT
  pid,
  command,
  type,
  bytes_processed,
  bytes_total,
  tuples_processed,
  CASE
    WHEN bytes_total > 0
    THEN round((bytes_processed::numeric / bytes_total::numeric) * 100, 2)
    ELSE NULL
  END AS percent
FROM pg_stat_progress_copy;
\watch 5
```

### Locks

```sql
SELECT
  blocked.pid AS blocked_pid,
  blocking.pid AS blocking_pid,
  blocked_activity.query AS blocked_query,
  blocking_activity.query AS blocking_query
FROM pg_catalog.pg_locks blocked
JOIN pg_catalog.pg_stat_activity blocked_activity
  ON blocked_activity.pid = blocked.pid
JOIN pg_catalog.pg_locks blocking
  ON blocking.locktype = blocked.locktype
 AND blocking.database IS NOT DISTINCT FROM blocked.database
 AND blocking.relation IS NOT DISTINCT FROM blocked.relation
 AND blocking.page IS NOT DISTINCT FROM blocked.page
 AND blocking.tuple IS NOT DISTINCT FROM blocked.tuple
 AND blocking.virtualxid IS NOT DISTINCT FROM blocked.virtualxid
 AND blocking.transactionid IS NOT DISTINCT FROM blocked.transactionid
 AND blocking.classid IS NOT DISTINCT FROM blocked.classid
 AND blocking.objid IS NOT DISTINCT FROM blocked.objid
 AND blocking.objsubid IS NOT DISTINCT FROM blocked.objsubid
 AND blocking.pid <> blocked.pid
JOIN pg_catalog.pg_stat_activity blocking_activity
  ON blocking_activity.pid = blocking.pid
WHERE NOT blocked.granted;
```

### Tamanhos das tabelas principais

```sql
SELECT
  relname AS table_name,
  pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM pg_catalog.pg_statio_user_tables
WHERE relname IN (
  'empresas',
  'estabelecimentos',
  'socios',
  'simples',
  'estabelecimento_cnaes_secundarios'
)
ORDER BY pg_total_relation_size(relid) DESC;
```

### Estimativa de linhas por tabela

```sql
SELECT
  relname AS table_name,
  n_live_tup AS estimated_rows,
  n_dead_tup AS dead_rows,
  last_analyze,
  last_autoanalyze
FROM pg_stat_user_tables
ORDER BY n_live_tup DESC;
```

### Logs do PostgreSQL no Windows

```powershell
Get-ChildItem "C:\Program Files\PostgreSQL\16\data\log" |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1 |
  Get-Content -Tail 120
```

Logs do Visualizador de Eventos via PowerShell:

```powershell
Get-EventLog -LogName Application -Newest 80 |
  Where-Object { $_.Source -like "*postgres*" -or $_.Message -like "*PostgreSQL*" } |
  Format-List TimeGenerated, Source, EntryType, Message
```

## Uso no Windows

No Windows, o script usa `\copy`, não o `COPY` do lado do servidor.

Isso é intencional. Com `\copy`, o cliente `psql` lê os arquivos locais e os envia em stream para o PostgreSQL. Isso evita problemas comuns de permissão do serviço no Windows, em que o usuário do serviço PostgreSQL não consegue ler arquivos do seu diretório de trabalho.

Exemplo:

```powershell
psql -d "postgres://postgres:postgres@localhost:5432/cnpj" -f "D:/cnpj-data/2026-05/postgres-direct/import-postgres-direct.sql"
```

## Benchmark de comparação recomendado

Para comparar os caminhos padrão e híbrido:

```bash
# Caminho padrão
cnpj-db-loader import ./downloads/<referencia>/sanitized --load-batch-size 500 --materialize-batch-size 50000 --verbose-progress

# Caminho híbrido
cnpj-db-loader postgres generate-script ./downloads/<referencia>/sanitized --output ./downloads/<referencia>/postgres-direct --source-encoding UTF8 --transaction-mode phase --force
psql -d "postgres://postgres:postgres@localhost:5432/cnpj" -f ./downloads/<referencia>/postgres-direct/import-postgres-direct.sql
```

Compare a duração total, o uso de disco, o uso de CPU do PostgreSQL, o crescimento do WAL e as contagens finais de linhas.
