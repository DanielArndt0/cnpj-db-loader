import {
  TABELA_ARQUIVOS_PLANO_IMPORTACAO,
  TABELA_CHECKPOINTS_IMPORTACAO,
  TABELA_CHECKPOINTS_MATERIALIZACAO,
  TABELA_ESTABELECIMENTO_CNAES_SECUNDARIOS,
  TABELA_ESTABELECIMENTOS,
  TABELA_PLANOS_IMPORTACAO,
  TABELA_QUARENTENA_IMPORTACAO,
  TABELA_SOCIOS,
} from "./table-names.js";

export function createIndexesSql(): string {
  return [
    "-- Índices operacionais",
    `create index if not exists idx_estabelecimentos_cnpj_basico on ${TABELA_ESTABELECIMENTOS} (cnpj_basico);`,
    `create index if not exists idx_estabelecimento_cnaes_secundarios_codigo_cnae on ${TABELA_ESTABELECIMENTO_CNAES_SECUNDARIOS} (codigo_cnae);`,
    `create index if not exists idx_socios_cnpj_basico on ${TABELA_SOCIOS} (cnpj_basico);`,
    `create index if not exists idx_planos_importacao_status on ${TABELA_PLANOS_IMPORTACAO} (status);`,
    `create index if not exists idx_planos_importacao_status_carga on ${TABELA_PLANOS_IMPORTACAO} (status_carga);`,
    `create index if not exists idx_planos_importacao_status_materializacao on ${TABELA_PLANOS_IMPORTACAO} (status_materializacao);`,
    `create index if not exists idx_arquivos_plano_importacao_plano_id on ${TABELA_ARQUIVOS_PLANO_IMPORTACAO} (plano_id);`,
    `create index if not exists idx_arquivos_plano_importacao_conjunto on ${TABELA_ARQUIVOS_PLANO_IMPORTACAO} (conjunto);`,
    `create index if not exists idx_checkpoints_importacao_status on ${TABELA_CHECKPOINTS_IMPORTACAO} (status);`,
    `create index if not exists idx_checkpoints_materializacao_status on ${TABELA_CHECKPOINTS_MATERIALIZACAO} (status);`,
    `create index if not exists idx_checkpoints_materializacao_plano_id on ${TABELA_CHECKPOINTS_MATERIALIZACAO} (plano_id);`,
    `create index if not exists idx_checkpoints_materializacao_conjunto on ${TABELA_CHECKPOINTS_MATERIALIZACAO} (conjunto);`,
    `create index if not exists idx_checkpoints_importacao_conjunto on ${TABELA_CHECKPOINTS_IMPORTACAO} (conjunto);`,
    `create index if not exists idx_quarentena_importacao_conjunto on ${TABELA_QUARENTENA_IMPORTACAO} (conjunto);`,
    `create index if not exists idx_quarentena_importacao_caminho_arquivo on ${TABELA_QUARENTENA_IMPORTACAO} (caminho_arquivo);`,
    `create index if not exists idx_quarentena_importacao_categoria_erro on ${TABELA_QUARENTENA_IMPORTACAO} (categoria_erro);`,
    `create index if not exists idx_quarentena_importacao_pode_tentar_novamente on ${TABELA_QUARENTENA_IMPORTACAO} (pode_tentar_novamente);`,
  ].join("\n");
}
