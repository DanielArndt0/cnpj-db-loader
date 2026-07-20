# Integração com a Receita Federal (rfb)

O grupo de comandos `rfb` automatiza a fase do dataset mensal de CNPJ remoto do compartilhamento público da Receita Federal do Brasil.

> O grupo foi renomeado de `federal-revenue` para `rfb` na versão 3.0.0. O nome antigo continua funcionando como alias nesta major, emitindo um aviso de depreciação, e será removido em uma versão futura.

Este recurso é aditivo. Não substitui os comandos locais estáveis. O comando `sync` completo usa os mesmos serviços internos já usados pelo fluxo manual:

```text
check/download -> extract -> validate -> sanitize -> import
```

O comando também tem o alias mais curto `revenue`.

O token do compartilhamento público da Receita Federal não é mais embutido no pacote. Configure-o localmente antes de usar os comandos remotos, ou passe-o com `--share-token` ao executar um comando.

## Comandos

```bash
cnpj-db-loader rfb config set share-token "<token-do-compartilhamento-publico>"
cnpj-db-loader rfb config show
cnpj-db-loader rfb config test
cnpj-db-loader rfb check
cnpj-db-loader rfb download --output ./downloads --force
cnpj-db-loader rfb status --output ./downloads
cnpj-db-loader rfb retry --output ./downloads --force
cnpj-db-loader rfb clean --output ./downloads --partials --force
cnpj-db-loader rfb sync --output ./downloads --db-url "postgresql://user:password@localhost:5432/cnpj" --force
```

Exemplos com o alias:

```bash
cnpj-db-loader revenue check
cnpj-db-loader revenue status 2026-05 --output ./downloads
cnpj-db-loader revenue retry 2026-05 --output ./downloads --force
```

## Configuração

As configurações WebDAV da Receita Federal são armazenadas no mesmo arquivo de config local do CNPJ DB Loader usado por `database config`. Isso mantém os valores específicos de endpoint fora do pacote npm publicado.

```bash
cnpj-db-loader rfb config set share-token "<token-do-compartilhamento-publico>"
cnpj-db-loader rfb config set webdav-url "https://arquivos.receitafederal.gov.br/public.php/webdav"
cnpj-db-loader rfb config set user-agent "cnpj-db-loader federal-revenue-client"
cnpj-db-loader rfb config show
cnpj-db-loader rfb config test
cnpj-db-loader rfb config reset share-token --force
cnpj-db-loader rfb config reset --force
```

Chaves de configuração:

| Chave         | Objetivo                                                                                    | Padrão                                                             |
| ------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| `share-token` | Token do compartilhamento público da Receita Federal usado na autenticação Basic do WebDAV. | Sem padrão. Deve ser configurado ou fornecido com `--share-token`. |
| `webdav-url`  | Endpoint WebDAV da Receita Federal.                                                         | `https://arquivos.receitafederal.gov.br/public.php/webdav`         |
| `user-agent`  | User agent HTTP usado nas requisições à Receita Federal.                                    | `cnpj-db-loader federal-revenue-client`                            |

Sobrescritas de linha de comando ainda têm prioridade sobre a configuração persistida:

```bash
cnpj-db-loader rfb check --share-token "<token>" --base-url "https://arquivos.receitafederal.gov.br/public.php/webdav"
```

## Seleção de referência

Por padrão, os comandos remotos listam o compartilhamento público e selecionam a última pasta disponível no formato `YYYY-MM`.

Use uma referência explícita quando precisar de um mês determinístico:

```bash
cnpj-db-loader rfb check --reference 2026-05
cnpj-db-loader rfb check 2026-05
cnpj-db-loader rfb download --reference 2026-05 --output ./downloads --force
cnpj-db-loader rfb download 2026-05 --output ./downloads --force
```

Use o mês calendário atual quando quiser que o comando falhe caso esse mês ainda não tenha sido publicado:

```bash
cnpj-db-loader rfb check --current
```

Se uma referência explícita ou atual não existir no compartilhamento público, o comando falha antes de listar ou baixar arquivos e informa a última referência disponível:

```text
VALIDATION_ERROR Referência da Receita Federal não encontrada: 2026-06. A última referência disponível é 2026-05.
```

Executar sem `--reference`, sem `[reference]` e sem `--current` mantém o comportamento padrão de selecionar a última referência publicada.

## Comportamento do download

`download` cria um subdiretório com o nome da referência selecionada dentro da raiz de saída configurada.

Por exemplo:

```bash
cnpj-db-loader rfb download --output ./downloads --reference 2026-05 --force
```

grava os arquivos em:

```text
./downloads/2026-05
```

O downloader:

- lista os arquivos `.zip` da referência mensal selecionada
- ignora um arquivo local completo quando o tamanho corresponde ao tamanho remoto
- grava as transferências incompletas como `<arquivo>.part`
- repete cada arquivo falho antes de marcá-lo como falho
- valida o tamanho local contra o tamanho remoto do WebDAV quando disponível
- grava um manifesto local para status, retry, clean e futura automação de runner
- usa `--overwrite` apenas quando um arquivo local completo deve ser baixado novamente

## Manifesto local

Cada referência baixada recebe um manifesto operacional local:

```text
<output>/<referencia>/.cnpj-db-loader/federal-revenue/manifest.json
```

O manifesto registra:

- referência selecionada
- URL base remota
- caminho de saída
- nomes e caminhos dos arquivos
- tamanho remoto e tamanho local
- status local: `downloaded`, `failed`, `partial` ou `missing`
- último comando e último status
- mensagem de erro quando um arquivo falha

Esse estado é intencionalmente local e baseado em arquivo. Não exige Redis nem PostgreSQL.

Durante o `sync`, a extração usa o motor 7-Zip embutido para dar suporte mais confiável a arquivos ZIP/ZIP64 grandes e volumes ZIP divididos. A saída da extração é preparada em um diretório temporário e promovida apenas após o arquivo ser processado com sucesso.

## Status

Use `status` para inspecionar o estado da referência local sem iniciar um novo download:

```bash
cnpj-db-loader rfb status 2026-05 --output ./downloads
```

O comando sai com código `0` quando a referência local está completa. Sai com código `1` quando o manifesto está ausente ou quando pelo menos um arquivo está falho, parcial ou ausente.

## Retry

Use `retry` para baixar apenas os arquivos incompletos registrados no manifesto:

```bash
cnpj-db-loader rfb retry 2026-05 --output ./downloads --force
```

Os arquivos completos são mantidos. Arquivos falhos, parciais e ausentes são repetidos conforme `--retries`.

## Clean

Use `clean` para a manutenção local:

```bash
cnpj-db-loader rfb clean 2026-05 --output ./downloads --partials --force
cnpj-db-loader rfb clean 2026-05 --output ./downloads --failed --force
cnpj-db-loader rfb clean 2026-05 --output ./downloads --all --force
```

Modos de limpeza:

| Modo         | Comportamento                                                                                  |
| ------------ | ---------------------------------------------------------------------------------------------- |
| `--partials` | Remove apenas os arquivos `.part`.                                                             |
| `--failed`   | Remove os arquivos falhos e parciais registrados no manifesto e depois os marca como ausentes. |
| `--all`      | Remove toda a pasta local da referência, incluindo arquivos ZIP e o estado do manifesto.       |

Apenas um modo de limpeza pode ser usado por vez.

## Lock de sync

`sync` cria um arquivo de lock local antes de executar o pipeline completo:

```text
<output>/<referencia>/.cnpj-db-loader/federal-revenue/sync.lock
```

Isso evita que dois processos de sync usem a mesma pasta de referência ao mesmo tempo.

Se um processo anterior foi interrompido e o lock está obsoleto, use `--force-lock` apenas após confirmar que nenhum outro sync está em execução:

```bash
cnpj-db-loader rfb sync 2026-05 --output ./downloads --force-lock --force
```

## Sync completo

`sync` executa o download remoto e depois o pipeline local do loader:

```bash
cnpj-db-loader rfb sync \
  --output ./downloads \
  --db-url "postgresql://user:password@localhost:5432/cnpj" \
  --load-batch-size 500 \
  --materialize-batch-size 50000 \
  --verbose-progress \
  --force
```

Diretórios de saída personalizados podem ser usados quando uma automação precisa de caminhos fixos:

```bash
cnpj-db-loader rfb sync \
  --reference 2026-05 \
  --output ./downloads \
  --extract-output ./work/2026-05/extracted \
  --sanitize-output ./work/2026-05/sanitized \
  --force
```

## Exit codes

| Comando    | Exit code `0`                                                | Exit code `1`                                                            |
| ---------- | ------------------------------------------------------------ | ------------------------------------------------------------------------ |
| `check`    | Referência e lista de arquivos foram resolvidas.             | Referência inválida, referência remota ausente ou erro de WebDAV.        |
| `download` | Download concluído sem arquivos falhos.                      | Um ou mais arquivos falharam.                                            |
| `status`   | O manifesto local existe e todos os arquivos foram baixados. | Manifesto ausente ou pelo menos um arquivo falho/parcial/ausente.        |
| `retry`    | Retry finalizado sem arquivos falhos/parciais/ausentes.      | Pelo menos um arquivo ainda está falho, parcial ou ausente.              |
| `clean`    | Limpeza concluída.                                           | Modo de limpeza inválido ou referência inválida.                         |
| `sync`     | Pipeline completo concluído.                                 | Falha de download, extração, validação, sanitização, importação ou lock. |

## Opções

| Opção                                   | Aplica-se a                                             | Objetivo                                                                     |
| --------------------------------------- | ------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `[reference]` / `--reference <yyyy-mm>` | `check`, `download`, `status`, `retry`, `clean`, `sync` | Seleciona uma referência mensal específica.                                  |
| `--current`                             | `check`, `download`, `status`, `retry`, `clean`, `sync` | Seleciona o mês calendário atual.                                            |
| `--output <path>`                       | `download`, `status`, `retry`, `clean`, `sync`          | Diretório raiz de download. A pasta da referência é criada dentro dele.      |
| `--retries <number>`                    | `download`, `retry`, `sync`                             | Tentativas de repetição por arquivo. Padrão: 3.                              |
| `--overwrite`                           | `download`, `retry`, `sync`                             | Baixa os arquivos novamente mesmo quando já existe uma cópia local completa. |
| `--partials`                            | `clean`                                                 | Remove apenas os arquivos `.part`.                                           |
| `--failed`                              | `clean`                                                 | Remove os arquivos falhos e parciais registrados no manifesto.               |
| `--all`                                 | `clean`                                                 | Remove toda a pasta local da referência.                                     |
| `--force-lock`                          | `sync`                                                  | Remove um lock de sync existente antes de iniciar.                           |
| `--extract-output <path>`               | `sync`                                                  | Diretório de saída de extração personalizado.                                |
| `--sanitize-output <path>`              | `sync`                                                  | Diretório de saída de sanitização personalizado.                             |
| `--db-url <url>`                        | `sync`                                                  | Sobrescreve a URL PostgreSQL salva para a fase de importação.                |
| `--dataset <dataset>`                   | `sync`                                                  | Restringe a fase de importação a um dataset.                                 |
| `--load-batch-size <size>`              | `sync`                                                  | Tamanho do lote de carga da importação.                                      |
| `--materialize-batch-size <size>`       | `sync`                                                  | Tamanho do bloco de materialização.                                          |
| `--verbose-progress`                    | `sync`                                                  | Mostra o progresso detalhado da importação.                                  |
| `--base-url <url>`                      | `check`, `download`, `status`, `retry`, `clean`, `sync` | Sobrescreve a URL base do WebDAV.                                            |
| `--share-token <token>`                 | `check`, `download`, `status`, `retry`, `clean`, `sync` | Sobrescreve o token do compartilhamento público.                             |
| `--user-agent <value>`                  | `check`, `download`, `status`, `retry`, `clean`, `sync` | Sobrescreve o user agent HTTP.                                               |
| `--force`                               | `download`, `retry`, `clean`, `sync`                    | Pula as confirmações interativas.                                            |

## Notas

Este módulo intencionalmente não inclui Redis, workers ou agendadores. A CLI continua responsável por operações determinísticas de uma única execução e pelo controle operacional local. Um futuro runner externo pode chamar esses comandos ou importar as funções de serviço públicas e adicionar enfileiramento, retries em nível de job, agendamento e notificações sem tornar o núcleo do loader mais pesado.
