import {
  TABELA_ARQUIVOS_PLANO_IMPORTACAO,
  TABELA_CHECKPOINTS_IMPORTACAO,
  TABELA_CHECKPOINTS_MATERIALIZACAO,
  TABELA_PLANOS_IMPORTACAO,
  TABELA_QUARENTENA_IMPORTACAO,
} from "./table-names.js";

export function createImportPlansSql(): string {
  return [
    `create table if not exists ${TABELA_PLANOS_IMPORTACAO} (`,
    "  id bigserial primary key,",
    "  impressao_digital_origem text not null unique,",
    "  caminho_entrada text not null,",
    "  caminho_validado text not null,",
    "  tamanho_lote integer not null,",
    "  banco_destino text not null,",
    "  total_conjuntos integer not null,",
    "  total_arquivos integer not null,",
    "  total_linhas bigint not null,",
    "  total_lotes bigint not null,",
    "  ordem_execucao jsonb not null,",
    "  status text not null default 'planned',",
    "  status_carga text not null default 'pending',",
    "  status_materializacao text not null default 'pending',",
    "  ultima_fase text,",
    "  ultimo_erro text,",
    "  criado_em timestamp with time zone not null default now(),",
    "  atualizado_em timestamp with time zone not null default now(),",
    "  ultimo_uso_em timestamp with time zone not null default now()",
    ");",
  ].join("\n");
}

export function createImportPlanFilesSql(): string {
  return [
    `create table if not exists ${TABELA_ARQUIVOS_PLANO_IMPORTACAO} (`,
    "  id bigserial primary key,",
    `  plano_id bigint not null references ${TABELA_PLANOS_IMPORTACAO} (id) on delete cascade,`,
    "  conjunto text not null,",
    "  indice_conjunto integer not null,",
    "  indice_arquivo integer not null,",
    "  caminho_arquivo text not null,",
    "  caminho_exibicao_arquivo text not null,",
    "  tamanho_arquivo bigint not null,",
    "  modificado_em timestamp with time zone not null,",
    "  total_linhas bigint not null,",
    "  total_lotes bigint not null,",
    "  unique (plano_id, caminho_arquivo)",
    ");",
  ].join("\n");
}

export function createImportCheckpointsSql(): string {
  return [
    `create table if not exists ${TABELA_CHECKPOINTS_IMPORTACAO} (`,
    "  id bigserial primary key,",
    "  conjunto text not null,",
    "  caminho_arquivo text not null,",
    "  tamanho_arquivo bigint not null,",
    "  modificado_em timestamp with time zone not null,",
    "  deslocamento_bytes bigint not null default 0,",
    "  linhas_confirmadas bigint not null default 0,",
    "  status text not null default 'pending',",
    "  ultimo_erro text,",
    "  atualizado_em timestamp with time zone not null default now(),",
    "  unique (conjunto, caminho_arquivo)",
    ");",
  ].join("\n");
}

export function createImportMaterializationCheckpointsSql(): string {
  return [
    `create table if not exists ${TABELA_CHECKPOINTS_MATERIALIZACAO} (`,
    "  id bigserial primary key,",
    `  plano_id bigint not null references ${TABELA_PLANOS_IMPORTACAO} (id) on delete cascade,`,
    "  conjunto text not null,",
    "  tabela_destino text not null,",
    "  status text not null default 'pending',",
    "  linhas_materializadas bigint not null default 0,",
    "  ultimo_staging_id bigint not null default 0,",
    "  blocos_concluidos bigint not null default 0,",
    "  ultimo_erro text,",
    "  iniciado_em timestamp with time zone,",
    "  concluido_em timestamp with time zone,",
    "  atualizado_em timestamp with time zone not null default now(),",
    "  staging_linhas_verificado bigint,",
    "  staging_max_staging_id_verificado bigint,",
    "  staging_validado_em timestamp with time zone,",
    "  status_reconciliacao_dominio text not null default 'pending',",
    "  reconciliacao_dominio_linhas_verificado bigint,",
    "  reconciliacao_dominio_max_staging_id_verificado bigint,",
    "  reconciliacao_dominio_concluida_em timestamp with time zone,",
    "  ultimo_bloco_primeiro_staging_id bigint not null default 0,",
    "  ultimo_bloco_ultimo_staging_id bigint not null default 0,",
    "  ultimo_bloco_linhas bigint not null default 0,",
    "  unique (plano_id, conjunto)",
    ");",
  ].join("\n");
}

export function createImportQuarantineSql(): string {
  return [
    `create table if not exists ${TABELA_QUARENTENA_IMPORTACAO} (`,
    "  id bigserial primary key,",
    "  conjunto text not null,",
    "  caminho_arquivo text not null,",
    "  numero_linha bigint,",
    "  deslocamento_checkpoint bigint,",
    "  codigo_erro text,",
    "  categoria_erro text,",
    "  etapa_erro text,",
    "  mensagem_erro text not null,",
    "  linha_bruta text not null,",
    "  payload_parseado jsonb,",
    "  sanitizacoes_aplicadas jsonb,",
    "  total_tentativas integer not null default 0,",
    "  pode_tentar_novamente boolean not null default false,",
    "  criado_em timestamp with time zone not null default now()",
    ");",
  ].join("\n");
}

export function createControlSchemaParts(): string[] {
  return [
    "-- Tabelas de controle de importação",
    createImportPlansSql(),
    createImportPlanFilesSql(),
    createImportCheckpointsSql(),
    createImportMaterializationCheckpointsSql(),
    createImportQuarantineSql(),
  ];
}
