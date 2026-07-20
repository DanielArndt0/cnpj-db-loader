# Uso

## Fluxo recomendado

```bash
cnpj-db-loader inspect ./downloads
cnpj-db-loader extract ./downloads
cnpj-db-loader validate ./downloads/extracted
cnpj-db-loader sanitize ./downloads/extracted
cnpj-db-loader database config set "postgresql://user:password@localhost:5432/cnpj"
cnpj-db-loader schema generate --profile full
cnpj-db-loader import ./downloads/sanitized --load-batch-size 500 --materialize-batch-size 50000 --verbose-progress
cnpj-db-loader import load ./downloads/sanitized --load-batch-size 20000
cnpj-db-loader import materialize ./downloads/sanitized --materialize-batch-size 50000
```

## Download mensal da Receita Federal

O grupo de comandos `rfb` automatiza apenas a fase do dataset mensal remoto. Não substitui o pipeline existente do loader; `sync` reutiliza os mesmos serviços de extract, validate, sanitize e import que o fluxo manual usa.

```bash
cnpj-db-loader rfb check
cnpj-db-loader rfb download --output ./downloads --force
cnpj-db-loader rfb status --output ./downloads
cnpj-db-loader rfb retry --output ./downloads --force
cnpj-db-loader rfb sync --output ./downloads --db-url "postgresql://user:password@localhost:5432/cnpj" --force
```

Por padrão, a última pasta `YYYY-MM` publicada é selecionada no compartilhamento público da Receita Federal. Use `--current` para atingir o mês calendário atual, `--reference 2026-05`, ou o atalho posicional `rfb check 2026-05` para forçar uma referência específica. Os downloads são gravados em `<output>/<referencia>`, os arquivos locais completos são ignorados, as transferências em andamento usam arquivos `.part`, e o estado local é armazenado em `<output>/<referencia>/.cnpj-db-loader/federal-revenue/manifest.json`.

## O que cada passo faz

| Passo | Comando                                              | Objetivo                                                                                                                |
| ----- | ---------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| 0     | `rfb check/download/status/retry/clean/sync`         | Opcionalmente verifica, baixa, inspeciona, repete, limpa ou sincroniza os arquivos mensais de CNPJ antes do fluxo local |
| 1     | `inspect <input>`                                    | Detecta se a pasta contém arquivos ZIP, conteúdo extraído ou ambos                                                      |
| 2     | `extract <input>`                                    | Extrai os arquivos ZIP, ZIP64 e ZIP divididos da Receita para `./extracted` por padrão                                  |
| 3     | `validate <input>`                                   | Valida a árvore de dataset extraído e confirma que os blocos de dataset necessários estão presentes                     |
| 4     | `sanitize <input>`                                   | Normaliza os arquivos de origem da Receita em saída UTF-8 validada antes da importação                                  |
| 5     | `database config show` / `database config set <url>` | Revisa ou configura a conexão PostgreSQL                                                                                |
| 6     | `schema generate --profile full`                     | Gera o schema SQL combinado com tabelas finais, de controle e de staging                                                |
| 7     | `import <input>`                                     | Executa o pipeline completo: carga de staging/direta, materialização de staging e geração do resumo final               |
| 8     | `import load <input>`                                | Para após a fase de carga quando você quer o staging populado sem materializá-lo imediatamente                          |
| 9     | `import materialize <input>`                         | Retoma do plano salvo e materializa os datasets de staging no schema final em blocos                                    |

## Comportamento da extração

`extract` usa um motor 7-Zip embutido para o tratamento robusto de arquivos grandes da Receita. O extrator suporta arquivos ZIP regulares, arquivos ZIP64 e volumes ZIP divididos cujo primeiro arquivo termina com `.zip.001`. Conjuntos ZIP divididos tradicionais que terminam com um volume central `.zip` também continuam sendo descobertos normalmente.

Cada arquivo é primeiro extraído em um diretório temporário. A pasta de destino final é substituída apenas após a extração ser bem-sucedida, então arquivos interrompidos ou inválidos não deixam uma pasta parcial que pareça completa.

## Perfis de schema

Use o perfil do comando schema que corresponde ao formato de banco que você quer preparar:

- `full`: tabelas finais, tabelas de controle de importação e tabelas de staging
- `final`: apenas as tabelas relacionais finais e de controle
- `staging`: apenas as tabelas leves `staging_*` usadas pelas etapas de carga em massa de staging antes da materialização final

Exemplos:

```bash
cnpj-db-loader schema generate --profile full
cnpj-db-loader schema generate --profile final
cnpj-db-loader schema generate --profile staging
```

## Comportamento importante da importação

`import` foi projetado para ser seguro com grandes datasets. A CLI também expõe `import load`, `import materialize` e `database cleanup ...` para que as fases pesadas e as operações seguras de reset possam ser automatizadas separadamente.

- começa com uma varredura preparatória exata que conta as linhas de origem e os lotes planejados quando não existe plano salvo
- persiste o plano de importação no banco e o reutiliza na retomada quando os arquivos de origem validados e o tamanho de lote coincidem
- lê os arquivos em modo de streaming
- carrega os grandes datasets em tabelas de staging leves via COPY do PostgreSQL, com apenas normalização leve no hot path, e adia o trabalho mais pesado para a etapa de materialização em ordem de dependência
- durante a materialização de estabelecimentos, também popula `estabelecimento_cnaes_secundarios` a partir de `cnae_fiscal_secundaria_raw`, substituindo a antiga necessidade de um script de backfill separado do lado da API
- antes de cada dataset de staging ser materializado no schema final, o importador só reconcilia os códigos de lookup/domínio ausentes quando o schema final atual ainda exige essas chaves estrangeiras de lookup
- assim que a fase de importação de arquivos termina, o terminal muda para uma fase MATERIALIZANDO dedicada e o log de progresso JSONL emite entradas de heartbeat durante os upserts longos de staging para final
- ainda faz upsert dos datasets de domínio menores diretamente no schema final
- confirma por unidade de carga em vez de manter uma única transação gigante
- armazena o progresso de carga de arquivo em `checkpoints_importacao`
- armazena o progresso de materialização em `checkpoints_materializacao`
- as linhas que ainda falham na validação ou nas constraints do banco são gravadas em `quarentena_importacao` e ignoradas
- se um lote falha, reexecutar o mesmo comando retoma a partir do último deslocamento de bytes confirmado
- novos planos de importação truncam as tabelas de staging selecionadas antes da carga, enquanto planos retomados reutilizam as linhas de staging que já correspondem aos checkpoints salvos antes de a passagem de materialização final rodar novamente

## Configurações de importação recomendadas

Para grandes primeiras cargas, sanitize primeiro e depois comece com:

```bash
cnpj-db-loader sanitize ./downloads/extracted
cnpj-db-loader import ./downloads/sanitized --load-batch-size 500 --materialize-batch-size 50000 --verbose-progress
cnpj-db-loader import load ./downloads/sanitized --load-batch-size 20000
cnpj-db-loader import materialize ./downloads/sanitized --materialize-batch-size 50000
```

Aumente `--load-batch-size` apenas após confirmar que a sua instância PostgreSQL e o seu orçamento de memória aguentam unidades de carga COPY maiores. Use `--materialize-batch-size` para controlar quantas linhas de staging cada bloco de materialização processa antes de salvar um checkpoint de materialização. O plano de importação salvo mantém o tamanho de lote de carga original usado no planejamento/carga, então mudar apenas `--materialize-batch-size` não cria um novo plano; a interface mostra os dois valores separadamente durante execuções de retomada/materialização.

## Recomendações de PostgreSQL e Docker

Para uma máquina com 32 GB de RAM, comece de forma conservadora:

- `shared_buffers = 512MB` a `1GB`
- `work_mem = 8MB` a `16MB`
- `maintenance_work_mem = 256MB`
- garanta que o Docker Desktop não esteja superalocando memória ao contêiner

Estes são pontos de partida, não regras absolutas. A otimização mais segura ainda é manter `--load-batch-size` modesto até você validar os limites do seu PostgreSQL.

## Visibilidade do progresso da importação

`import` grava dois tipos de log dentro de `~/.cnpjdbloader/logs`:

- um log de resumo final em JSON
- um log de progresso incremental em JSONL para cada lote confirmado, fallback de retry, métricas de arquivo, métricas de dataset, resumo final de conclusão e falha de importação de nível superior quando a execução é abortada mais cedo

Cada log JSON e JSONL carrega um envelope estruturado com `timestamp`, `level`, `severity`, `event` e `kind`. Isso facilita filtrar eventos informativos versus avisos e erros em visualizadores JSON e extensões JSONL.

Use `--verbose-progress` quando quiser um bloco de status fixo de várias linhas com dataset, arquivo, linhas confirmadas, total de lotes e progresso do arquivo enquanto a importação está rodando.

O resumo final da importação também inclui métricas de referência de tempo de varredura preparatória, tempo de execução, tempo de insert, tempo de retry, tempo de quarentena, tempo de materialização, linhas por segundo e lotes por minuto.

A varredura preparatória exata roda apenas quando não existe plano de importação salvo para os mesmos arquivos de origem validados e o mesmo tamanho de lote. Na retomada, o importador reutiliza o plano salvo e depois reutiliza a tabela de checkpoint para continuar a partir do último deslocamento de bytes confirmado, em vez de reiniciar a própria carga de dados. As linhas que falham após as repetições são gravadas em `quarentena_importacao`, então algumas linhas ruins não interrompem o dataset inteiro. Rodar `sanitize` primeiro reduz a frequência com que o importador precisa recorrer a esses caminhos de recuperação mais lentos.

## Importação direta híbrida no PostgreSQL

Após a sanitização, você pode gerar um script `psql` direto e deixar o PostgreSQL carregar os arquivos sanitizados da Receita sem reescrever o dataset completo em outra árvore de CSV:

```bash
cnpj-db-loader sanitize ./downloads/<referencia>/extracted
cnpj-db-loader postgres generate-script ./downloads/<referencia>/sanitized --output ./downloads/<referencia>/postgres-direct --force
psql "postgres://postgres:postgres@localhost:5432/cnpj" -f ./downloads/<referencia>/postgres-direct/import-postgres-direct.sql
```

Use esse fluxo quando quiser que o PostgreSQL faça a carga pesada em massa e a materialização baseada em conjuntos diretamente. Use `postgres export-csv` apenas quando precisar de uma árvore de CSV normalizada intermediária para fins de auditoria/debug. Use o comando `import` padrão quando precisar de retomada por checkpoint e recuperação por quarentena de linhas.

Veja [Importação direta no PostgreSQL](./postgres-direct.md) para detalhes.

## Análise da quarentena

Use o serviço `quarantine` após uma importação demorada quando quiser inspecionar as linhas que não puderam ser inseridas.

```bash
cnpj-db-loader quarantine stats
cnpj-db-loader quarantine list --dataset establishments --limit 20
cnpj-db-loader quarantine show 42
```

`quarantine stats` é útil para entender a escala de um problema por dataset, categoria de erro ou etapa de erro.

`quarantine list` é útil para paginar as linhas com filtros como `--retryable`, `--terminal`, `--category` e `--stage`.

`quarantine show` carrega uma linha em quarentena em detalhe, incluindo a linha bruta e o payload parseado quando disponíveis.

## Comandos de manutenção do banco

A família de comandos `database` separa a configuração de conexão das ações destrutivas de manutenção:

```bash
cnpj-db-loader database config show
cnpj-db-loader database cleanup staging --validated-path ./downloads/sanitized --force
cnpj-db-loader database cleanup materialized --dataset companies --force
cnpj-db-loader database cleanup checkpoints --phase all --validated-path ./downloads/sanitized --force
cnpj-db-loader database cleanup plans --validated-path ./downloads/sanitized --force
```

Use `--force` para pular as confirmações interativas. Sem ela, os comandos de limpeza sempre perguntam antes de alterar o banco.

- A materialização armazena marcadores leves de validação de staging (contagem de linhas e maior staging id) na tabela de checkpoint de materialização, para que reexecuções verifiquem rapidamente o estado atual do staging e reutilizem a reconciliação de domínio quando o snapshot de staging não mudou. O runtime valida que as tabelas de importação necessárias já existem, mas não as cria nem as altera automaticamente.
