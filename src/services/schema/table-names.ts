import {
  ageGroupsLayout,
  branchTypesLayout,
  citiesLayout,
  cnaesLayout,
  companiesLayout,
  companySizesLayout,
  countriesLayout,
  establishmentsLayout,
  legalNaturesLayout,
  partnerQualificationsLayout,
  partnersLayout,
  partnerTypesLayout,
  reasonsLayout,
  registrationStatusesLayout,
  simplesLayout,
} from "../../dictionary/layouts/index.js";
import type { ImportDatasetType, LookupTableName } from "../import/types.js";

export const TABELA_EMPRESAS = companiesLayout.tableName;
export const TABELA_ESTABELECIMENTOS = establishmentsLayout.tableName;
export const TABELA_ESTABELECIMENTO_CNAES_SECUNDARIOS =
  "estabelecimento_cnaes_secundarios";
export const TABELA_SOCIOS = partnersLayout.tableName;
export const TABELA_SIMPLES = simplesLayout.tableName;

export const TABELA_PLANOS_IMPORTACAO = "planos_importacao";
export const TABELA_ARQUIVOS_PLANO_IMPORTACAO = "arquivos_plano_importacao";
export const TABELA_CHECKPOINTS_IMPORTACAO = "checkpoints_importacao";
export const TABELA_CHECKPOINTS_MATERIALIZACAO = "checkpoints_materializacao";
export const TABELA_QUARENTENA_IMPORTACAO = "quarentena_importacao";

export const TABELA_STAGING_EMPRESAS = "staging_empresas";
export const TABELA_STAGING_ESTABELECIMENTOS = "staging_estabelecimentos";
export const TABELA_STAGING_SOCIOS = "staging_socios";
export const TABELA_STAGING_SIMPLES = "staging_simples";

export const COLUNA_CNPJ_BASICO = "cnpj_basico";
export const COLUNA_CNPJ_ORDEM = "cnpj_ordem";
export const COLUNA_CNPJ_DV = "cnpj_dv";
export const COLUNA_CNPJ_COMPLETO = "cnpj_completo";
export const COLUNA_CODIGO_CNAE = "codigo_cnae";
export const COLUNA_CHAVE_DEDUPLICACAO_SOCIO = "chave_deduplicacao_socio";
export const COLUNA_CRIADO_EM = "criado_em";
export const COLUNA_ATUALIZADO_EM = "atualizado_em";
export const COLUNA_STAGING_ID = "staging_id";

export const COLUNA_CODIGO = "codigo";
export const COLUNA_DESCRICAO = "descricao";

export const NOMES_TABELAS_LOOKUP: Record<LookupTableName, string> = {
  partner_qualifications: partnerQualificationsLayout.tableName,
  legal_natures: legalNaturesLayout.tableName,
  company_sizes: companySizesLayout.tableName,
  branch_types: branchTypesLayout.tableName,
  registration_statuses: registrationStatusesLayout.tableName,
  reasons: reasonsLayout.tableName,
  countries: countriesLayout.tableName,
  cnaes: cnaesLayout.tableName,
  cities: citiesLayout.tableName,
  partner_types: partnerTypesLayout.tableName,
  age_groups: ageGroupsLayout.tableName,
};

export const NOMES_TABELAS_DATASET: Record<ImportDatasetType, string> = {
  companies: companiesLayout.tableName,
  establishments: establishmentsLayout.tableName,
  partners: partnersLayout.tableName,
  simples_options: simplesLayout.tableName,
  countries: countriesLayout.tableName,
  cities: citiesLayout.tableName,
  partner_qualifications: partnerQualificationsLayout.tableName,
  legal_natures: legalNaturesLayout.tableName,
  cnaes: cnaesLayout.tableName,
  reasons: reasonsLayout.tableName,
};

export const NOMES_TABELAS_STAGING: Partial<Record<ImportDatasetType, string>> =
  {
    companies: TABELA_STAGING_EMPRESAS,
    establishments: TABELA_STAGING_ESTABELECIMENTOS,
    partners: TABELA_STAGING_SOCIOS,
    simples_options: TABELA_STAGING_SIMPLES,
  };
