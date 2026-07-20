# Sanitização

## Objetivo

`sanitize` prepara uma árvore de dataset normalizada antes da importação no PostgreSQL.

Os arquivos de origem da Receita Federal podem usar o encoding legado `ISO-8859-1`. O comando normaliza os bytes de origem antes de qualquer parsing de texto posterior:

```txt
arquivo extraído bruto
  -> stream de bytes
  -> remove bytes NUL (0x00)
  -> decodifica o encoding de origem
  -> codifica como UTF-8
  -> grava arquivo UTF-8 temporário
  -> valida a saída normalizada
  -> substitui o arquivo de destino apenas após a validação ser bem-sucedida
```

Isso evita que caracteres acentuados do português sejam persistidos incorretamente antes da importação.

## Comando

```bash
cnpj-db-loader sanitize <input>
```

## Opções

| Opção                          | Descrição                                                                                                                                      |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `--output <path>`              | Diretório de saída personalizado para a árvore de dataset sanitizado.                                                                          |
| `--dataset <name>`             | Sanitiza apenas um bloco de dataset, como `establishments` ou `companies`.                                                                     |
| `--source-encoding <encoding>` | Encoding dos arquivos de origem usado ao ler os arquivos da Receita. Padrão: `LATIN1` (`ISO-8859-1`). Suportados: `LATIN1`, `WIN1252`, `UTF8`. |
| `--allow-replacement-chars`    | Permite caracteres de substituição Unicode (`�`) na saída em vez de falhar a validação. Use apenas para inspeção manual.                       |
| `-f, --force`                  | Pula a confirmação interativa.                                                                                                                 |

## Comportamento padrão de saída

- quando o caminho validado é `.../extracted`, a saída sanitizada padrão é `.../sanitized`;
- caso contrário, a saída padrão é `<caminho-validado>-sanitized`.

## Fluxo padrão recomendado

```bash
cnpj-db-loader inspect ./downloads
cnpj-db-loader extract ./downloads
cnpj-db-loader validate ./downloads/extracted
cnpj-db-loader sanitize ./downloads/extracted
cnpj-db-loader import ./downloads/sanitized --load-batch-size 500 --materialize-batch-size 50000 --verbose-progress
```

## Fluxo híbrido PostgreSQL recomendado

Os arquivos sanitizados são saída UTF-8 validada, então o script direto do PostgreSQL deve usar `UTF8` como encoding de origem.

```bash
cnpj-db-loader sanitize ./downloads/extracted --output ./downloads/sanitized --force
cnpj-db-loader postgres generate-script ./downloads/sanitized --output ./downloads/postgres-direct --source-encoding UTF8 --force
psql -d "postgres://postgres:postgres@localhost:5432/cnpj" -f ./downloads/postgres-direct/import-postgres-direct.sql
```

## Comportamento de normalização segura

O pipeline de sanitização:

- processa arquivos grandes como streams, em vez de carregá-los inteiramente na memória;
- remove bytes NUL em nível de byte antes de decodificar;
- usa `ISO-8859-1` como padrão para decodificar os arquivos de origem da Receita;
- grava a saída UTF-8 em um arquivo temporário;
- valida a saída temporária antes de substituir o arquivo de destino;
- preserva um arquivo de destino previamente válido quando a normalização ou a validação falha;
- rejeita caracteres de substituição Unicode (`�`) por padrão, em vez de removê-los silenciosamente;
- informa o encoding de origem, os bytes NUL removidos, os caracteres de controle removidos e as métricas de caracteres de substituição.

## Notas sobre encoding

O encoding de origem padrão é `LATIN1`, que corresponde a `ISO-8859-1` e aos arquivos da Receita validados durante o desenvolvimento.

Use `WIN1252` apenas ao processar uma árvore de origem que comprovadamente usa Windows-1252:

```bash
cnpj-db-loader sanitize ./downloads/extracted --source-encoding WIN1252 --output ./downloads/sanitized --force
```

Use `UTF8` apenas quando a árvore de entrada já for UTF-8 válido:

```bash
cnpj-db-loader sanitize ./downloads/extracted --source-encoding UTF8 --output ./downloads/sanitized --force
```

## Validação de caracteres de substituição

O caractere visível:

```txt
�
```

é o caractere de substituição Unicode. Uma vez que ele tenha substituído um caractere acentuado original e o conteúdo corrompido tenha sido persistido, o valor original não pode ser recuperado de forma confiável apenas a partir daquele arquivo.

Por esse motivo, a sanitização falha por padrão se restarem caracteres de substituição na saída normalizada. A flag opcional `--allow-replacement-chars` existe apenas para inspeção manual controlada e não deve ser usada em fluxos normais de importação.

## Notas

- `sanitize` não substitui a validação da árvore de dataset;
- `sanitize` preserva os nomes de arquivo e os caminhos relativos para que a lógica de importação existente continue detectando os datasets pelo nome;
- inconsistências de negócio em nível de linha que sobrevivem à normalização continuam tratadas pelo fluxo de quarentena de importação existente;
- nenhuma alteração de schema de banco é necessária para usar `sanitize`.
