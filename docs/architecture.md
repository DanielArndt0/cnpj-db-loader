# Arquitetura

## O que importa nesta versão

A CLI atual gira em torno de um trabalho prático: mover os dados de CNPJ da Receita Federal dos arquivos baixados para o PostgreSQL de forma segura.

## Camadas principais

| Pasta            | Objetivo                                                             |
| ---------------- | -------------------------------------------------------------------- |
| `src/cli`        | Registro de comandos e saída no terminal                             |
| `src/services`   | Comportamento real da aplicação usado pela CLI                       |
| `src/dictionary` | Definições de layout dos datasets derivadas do dicionário da Receita |
| `src/core`       | Erros, prompts e utilitários compartilhados                          |
| `src/config`     | Auxiliares e caminhos de configuração local                          |
| `src/tests`      | Testes automatizados de regressão organizados por área da aplicação  |

## Design da importação

O pipeline de importação usa:

- ordem determinística de datasets para respeitar as chaves estrangeiras
- uma varredura preparatória exata que conta o total de linhas de origem e os lotes planejados antes da primeira escrita
- leitura de arquivos em streaming para evitar carregar o dataset completo na RAM
- uma etapa opcional de sanitização em streaming que normaliza os arquivos de origem ISO-8859-1 em saída UTF-8 validada, remove bytes NUL antes de decodificar e substitui os arquivos de destino apenas após a validação ser bem-sucedida
- escritas de staging baseadas em COPY para os grandes datasets, seguidas da materialização de staging para final
- upserts seguros contra conflitos para os datasets de domínio menores
- `planos_importacao` e `arquivos_plano_importacao` para persistir os planos de importação exatos e evitar recontar os mesmos arquivos de origem na retomada
- `checkpoints_importacao` para retomar uma carga falha sem limpar o banco inteiro
- `checkpoints_materializacao` para retomar a consolidação de staging para final por dataset e bloco
- `quarentena_importacao` para armazenar as linhas inválidas e continuar importações demoradas
- um serviço `quarantine` dedicado para inspecionar as linhas de quarentena sem tocar no pipeline de importação
- unidades de carga conservadoras para reduzir a pressão de memória e evitar rollbacks gigantes
- compatibilidade com schemas finais simplificados que mantêm identificadores derivados como colunas comuns quando necessário
- verificações/downloads WebDAV remotos da Receita Federal, além de manifesto local, retry, limpeza, status e lock de sync como um serviço aditivo de pré-pipeline

## Módulos de importação

O importador é dividido em módulos focados para que trabalhos futuros de desempenho possam substituir partes do pipeline sem reescrever o comando inteiro:

- `planner`: seleciona os datasets, coleta os arquivos de origem, reutiliza ou cria os planos de importação persistidos
- `source-reader`: faz streaming dos arquivos validados por deslocamento de bytes para leituras seguras na retomada
- `parser`: converte as linhas brutas da Receita em arrays de campos delimitados
- `normalizer`: valida a contagem de campos e transforma as linhas parseadas em registros prontos para o banco
- `staging-writer`: escolhe o alvo de escrita atual e usa COPY para as cargas em massa de staging
- `materializer`: consolida os datasets de staging no schema relacional final com upserts ordenados e checkpoints de bloco retomáveis
- o materializador reconcilia os códigos de lookup/domínio ausentes dos datasets de staging antes dos upserts finais, para que falhas tardias de chave estrangeira não interrompam o fluxo de consolidação em domínios compatíveis com placeholder
- o progresso da materialização é exposto explicitamente ao reporter de progresso da CLI e aos logs de heartbeat JSONL, para que upserts finais demorados não pareçam travados
- `finalizer`: centraliza o rastreamento de desempenho e a geração do resumo da importação
- `checkpoint-manager`: é responsável pela retomada por checkpoint, pela persistência e pelos marcadores de arquivo falho
- `quarantine-writer`: armazena as linhas ruins sem interromper importações longas
- `runner`: orquestra o fluxo de importação atual mantendo o ponto de entrada do serviço pequeno

O projeto também gera tabelas de staging dedicadas para os grandes datasets. A CLI expõe tanto um comando de uma execução (`import`) quanto comandos divididos (`import load`, `import materialize`). A limpeza de staging é tratada explicitamente por `database cleanup staging`. O caminho de escrita envia os datasets pesados primeiro para as tabelas de staging com apenas normalização leve e depois os consolida em um schema final simplificado em ordem de dependência, mantendo os datasets de catálogo menores diretamente no schema final. O schema final permanece próximo do layout da Receita, expondo também `estabelecimento_cnaes_secundarios`, uma tabela auxiliar normalizada que permite a uma API de CNPJ consultar uma linha por CNAE secundário de estabelecimento sem exigir um backfill externo após cada carga.

## Schema de staging

O schema SQL gerado suporta tabelas `staging_*` leves para os grandes datasets que passam pelo fluxo de carga em massa de staging antes da materialização final controlada.

Essas tabelas de staging são intencionalmente:

- `UNLOGGED` para cargas mais rápidas com muita escrita
- sem chaves estrangeiras e índices secundários
- sem colunas geradas e constraints exclusivas de upsert
- moldadas para espelhar as linhas do dataset validado com o mínimo de overhead de insert
- equipadas com `staging_id` para que o materializador possa registrar o progresso dos blocos com segurança

## Pré-pipeline da Receita Federal

A integração com a Receita Federal é mantida intencionalmente como um módulo de pré-pipeline. Ela vive em `src/services/federal-revenue` e é exposta por `src/cli/commands/register-federal-revenue.ts`. O módulo é responsável por:

- listar as referências mensais `YYYY-MM` do compartilhamento público WebDAV
- selecionar a referência mensal mais recente, atual ou explícita
- listar apenas os arquivos `.zip` dentro da referência selecionada
- baixar os arquivos com arquivos temporários `.part`, tentativas de repetição e comportamento de ignorar já existentes
- gravar um manifesto local de referência com o estado dos arquivos baixados, falhos, parciais e ausentes
- expor `status`, `retry` e `clean` para que a automação possa inspecionar e reparar as referências locais com segurança
- usar um lock de sync local para que dois processos de sync completo não usem a mesma pasta de referência ao mesmo tempo
- entregar a pasta de download concluída aos serviços existentes de extração, validação, sanitização e importação durante o `rfb sync`

Redis, workers em segundo plano e agendadores não fazem parte deste módulo de CLI. Essas responsabilidades devem permanecer fora do loader caso um runner/orquestrador externo seja adicionado depois.

## Fluxo de execução atual

```text
rfb check/download/status/retry/clean/sync -> inspect -> extract -> validate -> sanitize -> db/schema -> import
```

## Fluxo interno de importação

```text
planner -> source-reader -> parser -> normalizer -> staging-writer -> materializer -> finalizer
                  |                              |
                  +-> checkpoint-manager         +-> quarantine-writer
                                                 +-> materialization-checkpoints
```

- A materialização armazena marcadores leves de validação de staging (contagem de linhas e maior staging id) na tabela de checkpoint de materialização, para que reexecuções verifiquem rapidamente o estado atual do staging e reutilizem a reconciliação de domínio quando o snapshot de staging não mudou. O runtime valida que as tabelas de importação necessárias já existem, mas não as cria nem as altera automaticamente.

## Fluxo de importação direta no PostgreSQL

O fluxo de importação direta no PostgreSQL é um caminho de execução híbrido. Ele mantém a detecção de arquivos, a validação e a sanitização no loader e depois gera scripts `psql` modulares que leem os arquivos sanitizados da Receita diretamente, sem reescrever o dataset completo em uma segunda árvore de CSV.

Os scripts híbridos reutilizam o mesmo modelo operacional do importador padrão: `planos_importacao`, `arquivos_plano_importacao`, `checkpoints_importacao`, `checkpoints_materializacao` e `quarentena_importacao`.

Os scripts gerados reiniciam as tabelas de staging, carregam os arquivos sanitizados com `\copy`, colocam em quarentena as inconsistências conhecidas de nível de linha, inserem as linhas válidas nas tabelas de staging, fazem upsert das tabelas de domínio e finais, materializam `estabelecimento_cnaes_secundarios`, atualizam os checkpoints leves e atualizam as estatísticas do planejador com `ANALYZE`.

Antes de qualquer caminho de importação começar, o serviço de sanitização processa os arquivos extraídos como streams de bytes, remove bytes NUL, decodifica o encoding de origem configurado (padrão `LATIN1` / `ISO-8859-1`), grava a saída UTF-8 temporária e valida que não restam caracteres de substituição Unicode antes de mover os arquivos para a árvore sanitizada.

## Motor de extração

A extração de arquivos é isolada atrás do serviço de extração e usa o motor 7-Zip embutido. Isso mantém a CLI multiplataforma enquanto melhora o suporte a arquivos ZIP/ZIP64 grandes e volumes ZIP divididos. O extrator processa um arquivo por vez e prepara a saída em um diretório temporário antes de movê-la para a árvore extraída final.

A etapa de descoberta de arquivos aceita arquivos `.zip` regulares e o primeiro volume de arquivos divididos `.zip.001`. Os volumes adicionais são consumidos pelo 7-Zip automaticamente e não são agendados como arquivos separados.
