export function createPartnerDedupeGeneratedExpression(): string {
  return [
    "md5(",
    "    coalesce(cnpj_basico, '') || '|' ||",
    "    coalesce(identificador_socio, '') || '|' ||",
    "    coalesce(nome_socio_razao_social, '') || '|' ||",
    "    coalesce(cnpj_cpf_socio, '') || '|' ||",
    "    coalesce(codigo_qualificacao_socio, '') || '|' ||",
    "    coalesce((data_entrada_sociedade - date '2000-01-01')::text, '') || '|' ||",
    "    coalesce(codigo_pais, '') || '|' ||",
    "    coalesce(cpf_representante_legal, '') || '|' ||",
    "    coalesce(nome_representante_legal, '') || '|' ||",
    "    coalesce(codigo_qualificacao_representante_legal, '') || '|' ||",
    "    coalesce(codigo_faixa_etaria, '')",
    "  )",
  ].join("\n");
}
