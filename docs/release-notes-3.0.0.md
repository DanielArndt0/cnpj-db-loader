# CNPJ DB Loader 3.0.0

Versão major. Introduz suporte ao CNPJ alfanumérico e migra o banco gerado e a
interface para português. Há quebras de compatibilidade em relação à 2.4.0.

## Mudanças incompatíveis

- **Banco em português.** Todas as tabelas, colunas, constraints e índices do
  banco gerado passam a usar nomes em português (`snake_case`, sem acentos).
  Exemplos: `companies` → `empresas`, `establishments` → `estabelecimentos`,
  `partners` → `socios`, `simples_options` → `simples`,
  `establishment_secondary_cnaes` → `estabelecimento_cnaes_secundarios`. As
  tabelas de domínio e as tabelas de controle de importação também foram
  traduzidas (por exemplo `import_plans` → `planos_importacao`,
  `import_quarantine` → `quarentena_importacao`).
- **Tipos textuais para CNPJ.** `cnpj_basico`, `cnpj_ordem`, `cnpj_dv` e
  `cnpj_completo` são colunas de texto, nunca numéricas, com `CHECK` de charset e
  comprimento (`cnpj_basico ~ '^[0-9A-Z]{8}$'`, `cnpj_ordem ~ '^[0-9A-Z]{4}$'`,
  `cnpj_dv ~ '^[0-9]{2}$'`, `cnpj_completo ~ '^[0-9A-Z]{12}[0-9]{2}$'`).
- **Grupo de comandos `rfb`.** O grupo `federal-revenue` foi renomeado para
  `rfb` (Receita Federal Brasileira). O nome antigo continua aceito como alias
  nesta major, emitindo um aviso de depreciação, e será removido em uma versão
  futura.
- **Sem camada de compatibilidade em inglês.** A 3.0.0 não cria views nem
  adaptadores em inglês para consumidores externos. Bancos criados pela 2.4.0
  não são compatíveis com a 3.0.0.

## Novos recursos

- **CNPJ alfanumérico.** Suporte completo ao novo formato: 8 posições de CNPJ
  básico e 4 de ordem alfanuméricas (`0-9A-Z`), mais 2 dígitos verificadores
  numéricos. O cálculo dos DVs usa `valor = código ASCII − 48` com os pesos
  oficiais. CNPJs puramente numéricos existentes continuam válidos. Exemplo
  oficial: `12.ABC.345/01DE-35`.
- **Domínio centralizado.** Normalização, validação, composição, formatação e
  cálculo de DV concentrados em um único módulo de domínio, sem `parseInt`,
  `Number`, `bigint` ou cast numérico sobre segmentos do CNPJ.

## CLI

- Comandos e flags permanecem em inglês.
- Novo grupo canônico: `cnpj-db-loader rfb check | download | sync | status |
retry | clean` (mais `rfb config`).
- Alias de compatibilidade: `cnpj-db-loader federal-revenue ...` ainda funciona e
  imprime, no stderr:
  `O comando "federal-revenue" foi renomeado para "rfb". Use "cnpj-db-loader rfb ..." nas próximas versões.`

## Banco e geração SQL

- O comando `schema generate`, o schema estático (`sql/schema.sql`), os
  builders TypeScript, as tabelas de staging/controle e o gerador do caminho
  híbrido `postgres generate-script` produzem exclusivamente o schema 3.0.0 em
  português.
- A composição de `cnpj_completo` preserva exatamente 8 + 4 + 2 posições, sem
  conversão numérica e sem perder zeros à esquerda.
- A deduplicação de estabelecimentos usa `cnpj_completo`; a de sócios usa
  `chave_deduplicacao_socio`.

## Migração da 2.4.0

Não há migração in-place automática. O fluxo recomendado é:

1. Fazer backup da base 2.x.
2. Não executar a 3.0.0 sobre produção sem homologação.
3. Gerar o novo SQL com `cnpj-db-loader schema generate`.
4. Criar um banco 3.0.0 separado e aplicar o SQL gerado.
5. Reimportar os dados com a CLI 3.0.0.
6. Validar contagens e amostras (consultando CNPJs numéricos e alfanuméricos).
7. Trocar ambientes apenas após validação externa a este trabalho.

## Limitações conhecidas

- Não há conversão automática de um banco 2.x para o schema 3.0.0: é necessário
  criar um banco novo e reimportar.
- O alias `federal-revenue` é transitório e será removido em uma major futura.
