# CNPJ DB Loader

O CNPJ DB Loader é uma CLI prática para preparar os conjuntos de dados públicos de CNPJ da Receita Federal do Brasil para o PostgreSQL.

A versão 3.0.0 adiciona suporte ao CNPJ alfanumérico (mantendo válidos os CNPJs numéricos existentes) e gera o banco com nomes de tabelas, colunas e comentários em português. Comandos e flags da CLI permanecem em inglês.

## Escopo atual

Esta versão foca no fluxo real de carga:

- inspecionar um diretório baixado
- configurar, verificar, baixar, repetir, limpar e inspecionar os arquivos ZIP mensais de CNPJ da Receita Federal a partir do compartilhamento público
- extrair os arquivos ZIP da Receita Federal, incluindo ZIP/ZIP64 grandes e volumes ZIP divididos
- validar uma árvore extraída
- normalizar os arquivos validados da Receita de ISO-8859-1 para UTF-8 validado antes da importação, removendo bytes NUL em nível de byte e caracteres de controle problemáticos
- imprimir ou gerar schemas SQL finais, de staging ou combinados
- configurar e testar a URL padrão do PostgreSQL
- importar os arquivos de dataset validados para o PostgreSQL com:
  - varredura preparatória exata de total de linhas e total de lotes antes do início da importação
  - planos de importação persistidos e reutilizados na retomada para a mesma entrada validada e o mesmo tamanho de lote
  - cargas em massa de staging para os grandes datasets via COPY do PostgreSQL
  - materialização automática de `estabelecimento_cnaes_secundarios` a partir dos dados de CNAE secundário dos estabelecimentos
  - upserts diretos no schema final para os datasets de domínio menores
  - retomada por checkpoint por arquivo e deslocamento de bytes
  - quarentena de linhas para registros inválidos ou que violam constraints, sem interromper a importação
- gerar um script de importação `psql` direto que carrega os arquivos sanitizados da Receita sem reescrever o dataset completo em outra árvore de CSV
- comandos de inspeção da quarentena para analisar as linhas armazenadas em `quarentena_importacao`

## Instalação

```bash
npm install
```

Durante o desenvolvimento:

```bash
npm run cli -- --help
```

## Início rápido

```bash
cnpj-db-loader rfb config set share-token "<token-do-compartilhamento-publico>"
cnpj-db-loader rfb config test
cnpj-db-loader rfb check
cnpj-db-loader rfb download --output ./downloads
cnpj-db-loader rfb status --output ./downloads
cnpj-db-loader inspect ./downloads/<referencia>
cnpj-db-loader extract ./downloads/<referencia>
cnpj-db-loader validate ./downloads/<referencia>/extracted
cnpj-db-loader sanitize ./downloads/<referencia>/extracted
cnpj-db-loader database config set "postgresql://user:password@localhost:5432/cnpj"
cnpj-db-loader schema generate --profile full
cnpj-db-loader import ./downloads/<referencia>/sanitized --load-batch-size 500 --materialize-batch-size 50000 --verbose-progress

# Caminho híbrido opcional para carga direta no PostgreSQL
cnpj-db-loader postgres generate-script ./downloads/<referencia>/sanitized --output ./downloads/<referencia>/postgres-direct --source-encoding UTF8 --transaction-mode phase --force
psql -d "postgres://postgres:postgres@localhost:5432/cnpj" -f ./downloads/<referencia>/postgres-direct/import-postgres-direct.sql
```

> O grupo `federal-revenue` foi renomeado para `rfb`. O nome antigo continua funcionando como alias nesta major, emitindo um aviso de depreciação, e será removido em uma versão futura.

## Comandos estáveis

```bash
cnpj-db-loader rfb config set share-token "<token-do-compartilhamento-publico>"
cnpj-db-loader rfb config test
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
cnpj-db-loader postgres generate-script <input> [--output <path>] [--dataset <name>] [--script-name <name>] [--source-encoding <encoding>] [--transaction-mode <mode>] [--include <items>] [--skip-indexes] [--skip-analyze] [-f]
cnpj-db-loader postgres export-csv <input> [--output <path>] [--dataset <name>] [--script-name <name>] [-f]
cnpj-db-loader import <input> [--db-url <url>] [--dataset <name>] [--load-batch-size <size>] [--materialize-batch-size <size>] [--verbose-progress] [-f]
cnpj-db-loader import load <input> [--db-url <url>] [--dataset <name>] [--load-batch-size <size>] [--verbose-progress] [-f]
cnpj-db-loader import materialize <input> [--db-url <url>] [--dataset <name>] [--materialize-batch-size <size>] [--verbose-progress] [-f]
cnpj-db-loader doctor [--input <path>] [--db-url <url>]
cnpj-db-loader quarantine stats [--dataset <name>] [--category <name>] [--stage <name>] [--retryable] [--terminal]
cnpj-db-loader quarantine list [--dataset <name>] [--category <name>] [--stage <name>] [--retryable] [--terminal] [--limit <number>] [--after-id <id>]
cnpj-db-loader quarantine show <id> [--db-url <url>]
```

## Fluxo de importação direta no PostgreSQL

Para benchmarks locais ou cargas completas controladas, a CLI pode gerar um script de importação `psql` direto após a sanitização:

```bash
cnpj-db-loader sanitize ./downloads/<referencia>/extracted
cnpj-db-loader postgres generate-script ./downloads/<referencia>/sanitized --output ./downloads/<referencia>/postgres-direct --source-encoding UTF8 --transaction-mode phase --force
psql -d "postgres://postgres:postgres@localhost:5432/cnpj" -f ./downloads/<referencia>/postgres-direct/import-postgres-direct.sql
```

Esse caminho mantém download, extração, validação e sanitização robusta em UTF-8 dentro do loader e, em seguida, deixa o PostgreSQL carregar os arquivos sanitizados da Receita diretamente via `\copy`, validar inconsistências conhecidas em nível de linha, reutilizar a tabela `quarentena_importacao` existente, atualizar as tabelas de checkpoint de importação existentes, converter os valores válidos em tabelas de staging e materializar as tabelas finais com SQL baseado em conjuntos. O comando `import` padrão continua sendo o caminho retomável mais completo, enquanto o modo híbrido agora preserva a compatibilidade com as mesmas tabelas operacionais.

## Logs

Os logs de execução em JSON são gravados no diretório home do usuário em `~/.cnpjdbloader/logs`.

Cada entrada de log JSON e JSONL inclui um envelope estruturado com campos como `timestamp`, `level`, `severity`, `event` e `kind`. Logs de sucesso de comando são gravados com `status: "success"`, falhas de comando com `status: "failure"`, e eventos incrementais de progresso da importação são classificados com níveis como `debug`, `info`, `warning` e `error`.

Para o `import`, a CLI também grava um log de progresso incremental em JSONL com um evento por lote confirmado, fallback de retry, métricas de dataset, métricas de arquivo, falha de arquivo, resumo final de conclusão e falha de importação de nível superior quando a execução é abortada mais cedo.

O resumo final da importação inclui métricas de tempo e vazão de referência, como duração da varredura preparatória, duração da execução, tempo de insert, tempo de retry, tempo de quarentena, linhas por segundo e lotes por minuto.

Os internos da importação são divididos em módulos dedicados como planner, source reader, parser, normalizer, checkpoint manager, quarantine writer, staging writer, materializer e finalizer, para que mudanças na carga em massa de staging e na materialização final possam ser feitas sem reescrever o comando de importação inteiro.

A CLI também expõe um fluxo dividido: `import` executa o pipeline completo, `import load` para após as escritas de staging/direta, `import materialize` retoma do plano salvo e envia as linhas de staging para as tabelas finais, e `database cleanup ...` expõe comandos seguros de manutenção para tabelas de staging, tabelas finais materializadas simplificadas, checkpoints e planos salvos.

O progresso da materialização é registrado em checkpoint separado dos checkpoints de carga de arquivo, e o materializador trabalha em blocos retomáveis controlados por `--materialize-batch-size`. Durante etapas longas de materialização final, a CLI mantém a saída de progresso ao vivo em uma fase MATERIALIZANDO dedicada, reduzindo o custo de checkpoint e escrita JSONL por bloco para que os blocos retomáveis permaneçam rápidos. O schema final simplificado mantém o texto bruto de CNAE secundário nos estabelecimentos e também materializa `estabelecimento_cnaes_secundarios` para que APIs possam consultar uma linha por CNAE secundário sem rodar um script de backfill separado.

O serviço de extração usa um motor 7-Zip embutido para o processamento robusto de ZIP/ZIP64 grandes e suporte a volumes ZIP divididos. Os arquivos são extraídos em pastas temporárias e movidos para o local final apenas após uma extração bem-sucedida, evitando que pastas parcialmente extraídas sejam tratadas como completas.

O serviço de sanitização processa os arquivos de origem da Receita como streams, remove bytes NUL antes de decodificar, usa ISO-8859-1 como entrada padrão, grava a saída UTF-8 temporária e substitui o destino final apenas após a validação ser bem-sucedida. Caracteres de substituição Unicode são rejeitados por padrão para que texto corrompido não seja persistido silenciosamente antes da importação.

Os comandos `rfb` gravam os mesmos logs de comando estruturados e mantêm a fase de download remoto fora dos internos da importação. Arquivos ZIP já completos são ignorados por padrão, arquivos temporários `.part` são usados enquanto os downloads estão em andamento, e cada referência mantém um manifesto local para `status`, `retry`, `clean` e futura automação de runner.

O schema de banco gerado suporta três perfis:

- `full`: tabelas relacionais finais, tabelas de controle de importação e tabelas de staging
- `final`: apenas as tabelas relacionais finais e de controle
- `staging`: apenas as tabelas leves de staging usadas pelo fluxo de carga em massa de staging

`import --verbose-progress` mostra um bloco de status fixo de várias linhas em vez de poluir o terminal com uma nova linha a cada atualização de progresso.

## Documentação

- [Uso](./docs/usage.md)
- [Arquitetura](./docs/architecture.md)
- [Comandos](./docs/commands.md)
- [Quarentena](./docs/quarantine.md)
- [Sanitização](./docs/sanitize.md)
- [Receita Federal (rfb)](./docs/federal-revenue.md)
- [Importação direta no PostgreSQL](./docs/postgres-direct.md)
- [Notas da versão 3.0.0](./docs/release-notes-3.0.0.md)

A materialização armazena marcadores leves de validação de staging (contagem de linhas e maior staging id) na tabela de checkpoint de materialização, para que reexecuções verifiquem rapidamente o estado atual do staging e reutilizem a reconciliação de domínio quando o snapshot de staging não mudou. O runtime valida que as tabelas de importação necessárias já existem, mas não as cria nem as altera automaticamente.
