# CNPJ DB Loader

[![npm](https://img.shields.io/npm/v/@danielarndt0/cnpj-db-loader)](https://www.npmjs.com/package/@danielarndt0/cnpj-db-loader)
[![Node.js](https://img.shields.io/badge/Node.js-%3E%3D20-339933?logo=node.js&logoColor=white)](https://nodejs.org/)
[![Licença MIT](https://img.shields.io/badge/licen%C3%A7a-MIT-blue.svg)](./LICENSE)

CLI para baixar, preparar e importar os dados públicos de CNPJ da Receita Federal do Brasil para o PostgreSQL.

O CNPJ DB Loader cuida do fluxo completo: download, extração, validação, sanitização, criação do schema e importação dos dados.

> A versão 3.0.0 suporta CNPJs numéricos e alfanuméricos e gera tabelas e colunas em português.

## O que ele faz

- baixa os arquivos mensais de CNPJ da Receita Federal;
- extrai arquivos ZIP, ZIP64 e volumes divididos;
- valida a estrutura dos datasets;
- converte os arquivos de ISO-8859-1 para UTF-8;
- remove bytes NUL e caracteres de controle problemáticos;
- gera o schema PostgreSQL em português;
- importa arquivos grandes usando streaming e `COPY`;
- retoma importações interrompidas por checkpoints;
- envia registros inválidos para quarentena sem parar toda a carga;
- gera scripts `psql` para importação direta no PostgreSQL.

## Requisitos

- Node.js 20 ou superior;
- PostgreSQL;
- espaço em disco suficiente para os arquivos mensais e o banco gerado;
- `psql` apenas para aplicar o schema ou usar a importação direta.

O motor 7-Zip usado na extração já acompanha o pacote.

## Instalação

Instale globalmente pelo npm:

```bash
npm install -g @danielarndt0/cnpj-db-loader
```

Confira a instalação:

```bash
cnpj-db-loader --help
```

Também é possível usar o alias curto:

```bash
cdl --help
```

Sem instalação global:

```bash
npx @danielarndt0/cnpj-db-loader --help
```

## Início rápido

### 1. Configure o PostgreSQL

```bash
cnpj-db-loader database config set "postgresql://usuario:senha@localhost:5432/cnpj"
cnpj-db-loader database config test
```

### 2. Gere e aplique o schema

```bash
cnpj-db-loader schema generate \
  --profile full \
  --name cnpj-schema \
  --output ./sql
```

Aplique o arquivo SQL gerado:

```bash
psql "postgresql://usuario:senha@localhost:5432/cnpj" \
  -f ./sql/cnpj-schema.sql
```

### 3. Configure o acesso aos arquivos da Receita Federal

```bash
cnpj-db-loader rfb config set share-token "<token-do-compartilhamento-publico>"
cnpj-db-loader rfb config test
```

### 4. Baixe e importe a referência mais recente

O comando `sync` executa download, extração, validação, sanitização e importação:

```bash
cnpj-db-loader rfb sync \
  --output ./downloads \
  --db-url "postgresql://usuario:senha@localhost:5432/cnpj" \
  --verbose-progress \
  --force
```

## Fluxo manual

Use o fluxo manual quando quiser acompanhar ou executar cada etapa separadamente.

```bash
# Verifica a referência mensal disponível
cnpj-db-loader rfb check

# Baixa os arquivos
cnpj-db-loader rfb download --output ./downloads --force

# Inspeciona e extrai a referência baixada
cnpj-db-loader inspect ./downloads/<referencia>
cnpj-db-loader extract ./downloads/<referencia>

# Valida e converte os arquivos para UTF-8
cnpj-db-loader validate ./downloads/<referencia>/extracted
cnpj-db-loader sanitize ./downloads/<referencia>/extracted

# Importa para o PostgreSQL
cnpj-db-loader import \
  ./downloads/<referencia>/sanitized \
  --load-batch-size 500 \
  --materialize-batch-size 50000 \
  --verbose-progress
```

Substitua `<referencia>` pela pasta mensal baixada, por exemplo `2026-05`.

## Importação retomável

O comando `import` salva planos e checkpoints no PostgreSQL.

Se a execução for interrompida, execute novamente o mesmo comando com os mesmos arquivos e o mesmo `--load-batch-size`. O loader continuará a partir do último ponto confirmado.

Também é possível separar a carga da materialização:

```bash
cnpj-db-loader import load \
  ./downloads/<referencia>/sanitized \
  --load-batch-size 20000 \
  --verbose-progress
```

```bash
cnpj-db-loader import materialize \
  ./downloads/<referencia>/sanitized \
  --materialize-batch-size 50000 \
  --verbose-progress
```

## Importação direta no PostgreSQL

Para cargas completas controladas, o loader pode gerar scripts modulares para o `psql`.

```bash
cnpj-db-loader postgres generate-script \
  ./downloads/<referencia>/sanitized \
  --output ./downloads/<referencia>/postgres-direct \
  --source-encoding UTF8 \
  --transaction-mode phase \
  --force
```

Execute o script gerado:

```bash
psql "postgresql://usuario:senha@localhost:5432/cnpj" \
  -f ./downloads/<referencia>/postgres-direct/import-postgres-direct.sql
```

O modo `phase` confirma cada etapa separadamente. Assim, uma falha posterior não desfaz fases já concluídas.

## Quarentena

Registros inválidos são armazenados em `quarentena_importacao`, permitindo que a carga continue.

Resumo dos erros:

```bash
cnpj-db-loader quarantine stats
```

Listar registros:

```bash
cnpj-db-loader quarantine list --limit 20
```

Ver um registro específico:

```bash
cnpj-db-loader quarantine show 42
```

Os comandos de quarentena são somente leitura.

## Principais comandos

| Comando | Finalidade |
| --- | --- |
| `rfb check` | Verifica as referências mensais disponíveis |
| `rfb download` | Baixa os arquivos da Receita Federal |
| `rfb sync` | Executa o fluxo completo automaticamente |
| `inspect` | Identifica o conteúdo de um diretório |
| `extract` | Extrai os arquivos compactados |
| `validate` | Valida a árvore de datasets |
| `sanitize` | Converte e valida os arquivos em UTF-8 |
| `schema generate` | Gera o schema PostgreSQL |
| `import` | Carrega e materializa os dados |
| `postgres generate-script` | Gera uma importação direta para o `psql` |
| `quarantine` | Consulta registros rejeitados |
| `doctor` | Verifica rapidamente o ambiente |

Para consultar todas as opções:

```bash
cnpj-db-loader --help
cnpj-db-loader <comando> --help
```

## Perfis de schema

| Perfil | Conteúdo |
| --- | --- |
| `full` | Tabelas finais, controles de importação e staging |
| `final` | Tabelas finais e controles de importação |
| `staging` | Apenas tabelas de staging |

Para o fluxo completo, use:

```bash
cnpj-db-loader schema generate --profile full
```

## CNPJ alfanumérico

A versão 3.0.0 aceita:

- 8 posições alfanuméricas no CNPJ básico;
- 4 posições alfanuméricas na ordem;
- 2 dígitos verificadores numéricos.

Exemplo:

```text
12.ABC.345/01DE-35
```

CNPJs exclusivamente numéricos continuam válidos.

## Banco em português

O schema 3.0.0 utiliza nomes em português:

| Antes | Agora |
| --- | --- |
| `companies` | `empresas` |
| `establishments` | `estabelecimentos` |
| `partners` | `socios` |
| `import_plans` | `planos_importacao` |
| `import_quarantine` | `quarentena_importacao` |

> Bancos criados pela versão 2.x não são compatíveis com o schema 3.0.0. Crie um banco novo e reimporte os dados.

## Logs

Os logs ficam em:

```text
~/.cnpjdbloader/logs
```

A importação gera logs estruturados em JSON e JSONL com progresso, erros, métricas e resumo da execução.

## Documentação

- [Guia de uso](https://github.com/DanielArndt0/cnpj-db-loader/blob/main/docs/usage.md)
- [Referência de comandos](https://github.com/DanielArndt0/cnpj-db-loader/blob/main/docs/commands.md)
- [Integração com a Receita Federal](https://github.com/DanielArndt0/cnpj-db-loader/blob/main/docs/federal-revenue.md)
- [Sanitização](https://github.com/DanielArndt0/cnpj-db-loader/blob/main/docs/sanitize.md)
- [Quarentena](https://github.com/DanielArndt0/cnpj-db-loader/blob/main/docs/quarantine.md)
- [Importação direta no PostgreSQL](https://github.com/DanielArndt0/cnpj-db-loader/blob/main/docs/postgres-direct.md)
- [Arquitetura](https://github.com/DanielArndt0/cnpj-db-loader/blob/main/docs/architecture.md)

## Licença

Distribuído sob a licença [MIT](./LICENSE).
