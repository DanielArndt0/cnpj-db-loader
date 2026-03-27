export function createPartnerDedupeGeneratedExpression(): string {
  return [
    "md5(",
    "    coalesce(cnpj_root, '') || '|' ||",
    "    coalesce(partner_type_code, '') || '|' ||",
    "    coalesce(partner_name, '') || '|' ||",
    "    coalesce(partner_document, '') || '|' ||",
    "    coalesce(partner_qualification_code, '') || '|' ||",
    "    coalesce((entry_date - date '2000-01-01')::text, '') || '|' ||",
    "    coalesce(country_code, '') || '|' ||",
    "    coalesce(legal_representative_document, '') || '|' ||",
    "    coalesce(legal_representative_name, '') || '|' ||",
    "    coalesce(legal_representative_qualification_code, '') || '|' ||",
    "    coalesce(age_group_code, '')",
    "  )",
  ].join("\n");
}
