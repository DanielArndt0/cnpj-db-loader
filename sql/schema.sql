-- Esquema PostgreSQL do CNPJ DB Loader

-- Perfil: full

-- Gerado a partir do modelo interno da Receita Federal.

begin;

-- Tabelas de domínio

create table if not exists paises (
  codigo text not null,
  descricao text not null,
  primary key (codigo)
);

create table if not exists municipios (
  codigo text not null,
  descricao text not null,
  primary key (codigo)
);

create table if not exists qualificacoes_socios (
  codigo text not null,
  descricao text not null,
  primary key (codigo)
);

create table if not exists naturezas_juridicas (
  codigo text not null,
  descricao text not null,
  primary key (codigo)
);

create table if not exists cnaes (
  codigo text not null,
  descricao text not null,
  primary key (codigo)
);

create table if not exists motivos_situacao_cadastral (
  codigo text not null,
  descricao text not null,
  primary key (codigo)
);

create table if not exists portes_empresa (
  codigo text not null,
  descricao text not null,
  primary key (codigo)
);

create table if not exists identificadores_matriz_filial (
  codigo text not null,
  descricao text not null,
  primary key (codigo)
);

create table if not exists situacoes_cadastrais (
  codigo text not null,
  descricao text not null,
  primary key (codigo)
);

create table if not exists identificadores_socio (
  codigo text not null,
  descricao text not null,
  primary key (codigo)
);

create table if not exists faixas_etarias (
  codigo text not null,
  descricao text not null,
  primary key (codigo)
);

-- Tabelas finais (simplificadas para materialização rápida na primeira carga)

create table if not exists empresas (
  cnpj_basico text not null,
  razao_social_nome_empresarial text not null,
  codigo_natureza_juridica text not null,
  codigo_qualificacao_responsavel text not null,
  capital_social numeric(18,2) not null,
  codigo_porte_empresa text not null,
  ente_federativo_responsavel text,
  criado_em timestamp without time zone not null default now(),
  atualizado_em timestamp without time zone not null default now(),
  primary key (cnpj_basico),
  constraint chk_empresas_cnpj_basico check (cnpj_basico ~ '^[0-9A-Z]{8}$')
);

create table if not exists estabelecimentos (
  cnpj_basico text not null,
  cnpj_ordem text not null,
  cnpj_dv text not null,
  identificador_matriz_filial text not null,
  nome_fantasia text,
  situacao_cadastral text not null,
  data_situacao_cadastral date,
  motivo_situacao_cadastral text,
  nome_cidade_exterior text,
  codigo_pais text,
  data_inicio_atividade date,
  cnae_fiscal_principal text not null,
  cnae_fiscal_secundaria_raw text,
  tipo_logradouro text,
  logradouro text,
  numero text,
  complemento text,
  bairro text,
  cep text,
  uf text,
  codigo_municipio text,
  ddd_1 text,
  telefone_1 text,
  ddd_2 text,
  telefone_2 text,
  ddd_fax text,
  fax text,
  correio_eletronico text,
  situacao_especial text,
  data_situacao_especial date,
  cnpj_completo text not null,
  criado_em timestamp without time zone not null default now(),
  atualizado_em timestamp without time zone not null default now(),
  primary key (cnpj_completo),
  constraint chk_estabelecimentos_cnpj_basico check (cnpj_basico ~ '^[0-9A-Z]{8}$'),
  constraint chk_estabelecimentos_cnpj_ordem check (cnpj_ordem ~ '^[0-9A-Z]{4}$'),
  constraint chk_estabelecimentos_cnpj_dv check (cnpj_dv ~ '^[0-9]{2}$'),
  constraint chk_estabelecimentos_cnpj_completo check (cnpj_completo ~ '^[0-9A-Z]{12}[0-9]{2}$')
);

create table if not exists estabelecimento_cnaes_secundarios (
  cnpj_completo text not null,
  codigo_cnae text not null,
  primary key (cnpj_completo, codigo_cnae),
  constraint chk_estabelecimento_cnaes_secundarios_cnpj_completo check (cnpj_completo ~ '^[0-9A-Z]{12}[0-9]{2}$')
);

create table if not exists socios (
  id bigserial primary key,
  cnpj_basico text not null,
  identificador_socio text not null,
  nome_socio_razao_social text not null,
  cnpj_cpf_socio text,
  codigo_qualificacao_socio text not null,
  data_entrada_sociedade date,
  codigo_pais text,
  cpf_representante_legal text,
  nome_representante_legal text,
  codigo_qualificacao_representante_legal text,
  codigo_faixa_etaria text,
  chave_deduplicacao_socio text not null,
  criado_em timestamp without time zone not null default now(),
  atualizado_em timestamp without time zone not null default now(),
  unique (chave_deduplicacao_socio),
  constraint chk_socios_cnpj_basico check (cnpj_basico ~ '^[0-9A-Z]{8}$')
);

create table if not exists simples (
  cnpj_basico text not null,
  opcao_simples text,
  data_opcao_simples date,
  data_exclusao_simples date,
  opcao_mei text,
  data_opcao_mei date,
  data_exclusao_mei date,
  criado_em timestamp without time zone not null default now(),
  atualizado_em timestamp without time zone not null default now(),
  primary key (cnpj_basico),
  constraint chk_simples_cnpj_basico check (cnpj_basico ~ '^[0-9A-Z]{8}$'),
  constraint chk_opcao_simples check (opcao_simples in ('S', 'N') or opcao_simples is null or opcao_simples = ''),
  constraint chk_opcao_mei check (opcao_mei in ('S', 'N') or opcao_mei is null or opcao_mei = '')
);

-- Tabelas de controle de importação

create table if not exists planos_importacao (
  id bigserial primary key,
  impressao_digital_origem text not null unique,
  caminho_entrada text not null,
  caminho_validado text not null,
  tamanho_lote integer not null,
  banco_destino text not null,
  total_conjuntos integer not null,
  total_arquivos integer not null,
  total_linhas bigint not null,
  total_lotes bigint not null,
  ordem_execucao jsonb not null,
  status text not null default 'planned',
  status_carga text not null default 'pending',
  status_materializacao text not null default 'pending',
  ultima_fase text,
  ultimo_erro text,
  criado_em timestamp with time zone not null default now(),
  atualizado_em timestamp with time zone not null default now(),
  ultimo_uso_em timestamp with time zone not null default now()
);

create table if not exists arquivos_plano_importacao (
  id bigserial primary key,
  plano_id bigint not null references planos_importacao (id) on delete cascade,
  conjunto text not null,
  indice_conjunto integer not null,
  indice_arquivo integer not null,
  caminho_arquivo text not null,
  caminho_exibicao_arquivo text not null,
  tamanho_arquivo bigint not null,
  modificado_em timestamp with time zone not null,
  total_linhas bigint not null,
  total_lotes bigint not null,
  unique (plano_id, caminho_arquivo)
);

create table if not exists checkpoints_importacao (
  id bigserial primary key,
  conjunto text not null,
  caminho_arquivo text not null,
  tamanho_arquivo bigint not null,
  modificado_em timestamp with time zone not null,
  deslocamento_bytes bigint not null default 0,
  linhas_confirmadas bigint not null default 0,
  status text not null default 'pending',
  ultimo_erro text,
  atualizado_em timestamp with time zone not null default now(),
  unique (conjunto, caminho_arquivo)
);

create table if not exists checkpoints_materializacao (
  id bigserial primary key,
  plano_id bigint not null references planos_importacao (id) on delete cascade,
  conjunto text not null,
  tabela_destino text not null,
  status text not null default 'pending',
  linhas_materializadas bigint not null default 0,
  ultimo_staging_id bigint not null default 0,
  blocos_concluidos bigint not null default 0,
  ultimo_erro text,
  iniciado_em timestamp with time zone,
  concluido_em timestamp with time zone,
  atualizado_em timestamp with time zone not null default now(),
  staging_linhas_verificado bigint,
  staging_max_staging_id_verificado bigint,
  staging_validado_em timestamp with time zone,
  status_reconciliacao_dominio text not null default 'pending',
  reconciliacao_dominio_linhas_verificado bigint,
  reconciliacao_dominio_max_staging_id_verificado bigint,
  reconciliacao_dominio_concluida_em timestamp with time zone,
  ultimo_bloco_primeiro_staging_id bigint not null default 0,
  ultimo_bloco_ultimo_staging_id bigint not null default 0,
  ultimo_bloco_linhas bigint not null default 0,
  unique (plano_id, conjunto)
);

create table if not exists quarentena_importacao (
  id bigserial primary key,
  conjunto text not null,
  caminho_arquivo text not null,
  numero_linha bigint,
  deslocamento_checkpoint bigint,
  codigo_erro text,
  categoria_erro text,
  etapa_erro text,
  mensagem_erro text not null,
  linha_bruta text not null,
  payload_parseado jsonb,
  sanitizacoes_aplicadas jsonb,
  total_tentativas integer not null default 0,
  pode_tentar_novamente boolean not null default false,
  criado_em timestamp with time zone not null default now()
);

-- Tabelas de staging para importações em massa

create unlogged table if not exists staging_empresas (
  staging_id bigserial primary key,
  cnpj_basico text not null,
  razao_social_nome_empresarial text not null,
  codigo_natureza_juridica text not null,
  codigo_qualificacao_responsavel text not null,
  capital_social numeric(18,2) not null,
  codigo_porte_empresa text not null,
  ente_federativo_responsavel text
);

create unlogged table if not exists staging_estabelecimentos (
  staging_id bigserial primary key,
  cnpj_basico text not null,
  cnpj_ordem text not null,
  cnpj_dv text not null,
  identificador_matriz_filial text not null,
  nome_fantasia text,
  situacao_cadastral text not null,
  data_situacao_cadastral date,
  motivo_situacao_cadastral text,
  nome_cidade_exterior text,
  codigo_pais text,
  data_inicio_atividade date,
  cnae_fiscal_principal text not null,
  cnae_fiscal_secundaria_raw text,
  tipo_logradouro text,
  logradouro text,
  numero text,
  complemento text,
  bairro text,
  cep text,
  uf text,
  codigo_municipio text,
  ddd_1 text,
  telefone_1 text,
  ddd_2 text,
  telefone_2 text,
  ddd_fax text,
  fax text,
  correio_eletronico text,
  situacao_especial text,
  data_situacao_especial date
);

create unlogged table if not exists staging_socios (
  staging_id bigserial primary key,
  cnpj_basico text not null,
  identificador_socio text not null,
  nome_socio_razao_social text not null,
  cnpj_cpf_socio text,
  codigo_qualificacao_socio text not null,
  data_entrada_sociedade date,
  codigo_pais text,
  cpf_representante_legal text,
  nome_representante_legal text,
  codigo_qualificacao_representante_legal text,
  codigo_faixa_etaria text
);

create unlogged table if not exists staging_simples (
  staging_id bigserial primary key,
  cnpj_basico text not null,
  opcao_simples text,
  data_opcao_simples date,
  data_exclusao_simples date,
  opcao_mei text,
  data_opcao_mei date,
  data_exclusao_mei date
);

-- Dados iniciais das tabelas de domínio

insert into portes_empresa (codigo, descricao) values
  ('00', 'Não informado'),
  ('01', 'Micro empresa'),
  ('03', 'Empresa de pequeno porte'),
  ('05', 'Demais')
on conflict (codigo) do update set descricao = excluded.descricao;

insert into identificadores_matriz_filial (codigo, descricao) values
  ('1', 'Matriz'),
  ('2', 'Filial')
on conflict (codigo) do update set descricao = excluded.descricao;

insert into situacoes_cadastrais (codigo, descricao) values
  ('01', 'Nula'),
  ('2', 'Ativa'),
  ('3', 'Suspensa'),
  ('4', 'Inapta'),
  ('08', 'Baixada')
on conflict (codigo) do update set descricao = excluded.descricao;

insert into identificadores_socio (codigo, descricao) values
  ('1', 'Pessoa jurídica'),
  ('2', 'Pessoa física'),
  ('3', 'Estrangeiro')
on conflict (codigo) do update set descricao = excluded.descricao;

insert into faixas_etarias (codigo, descricao) values
  ('0', 'Não se aplica'),
  ('1', '0 a 12 anos'),
  ('2', '13 a 20 anos'),
  ('3', '21 a 30 anos'),
  ('4', '31 a 40 anos'),
  ('5', '41 a 50 anos'),
  ('6', '51 a 60 anos'),
  ('7', '61 a 70 anos'),
  ('8', '71 a 80 anos'),
  ('9', 'Maiores de 80 anos')
on conflict (codigo) do update set descricao = excluded.descricao;

-- Índices operacionais
create index if not exists idx_estabelecimentos_cnpj_basico on estabelecimentos (cnpj_basico);
create index if not exists idx_estabelecimento_cnaes_secundarios_codigo_cnae on estabelecimento_cnaes_secundarios (codigo_cnae);
create index if not exists idx_socios_cnpj_basico on socios (cnpj_basico);
create index if not exists idx_planos_importacao_status on planos_importacao (status);
create index if not exists idx_planos_importacao_status_carga on planos_importacao (status_carga);
create index if not exists idx_planos_importacao_status_materializacao on planos_importacao (status_materializacao);
create index if not exists idx_arquivos_plano_importacao_plano_id on arquivos_plano_importacao (plano_id);
create index if not exists idx_arquivos_plano_importacao_conjunto on arquivos_plano_importacao (conjunto);
create index if not exists idx_checkpoints_importacao_status on checkpoints_importacao (status);
create index if not exists idx_checkpoints_materializacao_status on checkpoints_materializacao (status);
create index if not exists idx_checkpoints_materializacao_plano_id on checkpoints_materializacao (plano_id);
create index if not exists idx_checkpoints_materializacao_conjunto on checkpoints_materializacao (conjunto);
create index if not exists idx_checkpoints_importacao_conjunto on checkpoints_importacao (conjunto);
create index if not exists idx_quarentena_importacao_conjunto on quarentena_importacao (conjunto);
create index if not exists idx_quarentena_importacao_caminho_arquivo on quarentena_importacao (caminho_arquivo);
create index if not exists idx_quarentena_importacao_categoria_erro on quarentena_importacao (categoria_erro);
create index if not exists idx_quarentena_importacao_pode_tentar_novamente on quarentena_importacao (pode_tentar_novamente);

commit;
