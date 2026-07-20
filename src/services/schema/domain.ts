import {
  ageGroupsLayout,
  branchTypesLayout,
  citiesLayout,
  cnaesLayout,
  companySizesLayout,
  countriesLayout,
  legalNaturesLayout,
  partnerQualificationsLayout,
  partnerTypesLayout,
  reasonsLayout,
  registrationStatusesLayout,
} from "../../dictionary/layouts/index.js";
import { createLookupSeedSql, createSimpleDomainTableSql } from "./shared.js";
import { NOMES_TABELAS_LOOKUP } from "./table-names.js";

const domainTables = [
  countriesLayout,
  citiesLayout,
  partnerQualificationsLayout,
  legalNaturesLayout,
  cnaesLayout,
  reasonsLayout,
  companySizesLayout,
  branchTypesLayout,
  registrationStatusesLayout,
  partnerTypesLayout,
  ageGroupsLayout,
];

export function createDomainSchemaParts(): string[] {
  return [
    "-- Tabelas de domínio",
    ...domainTables.map(createSimpleDomainTableSql),
  ];
}

export function createDomainSeedParts(): string[] {
  return [
    "-- Dados iniciais das tabelas de domínio",
    createLookupSeedSql(NOMES_TABELAS_LOOKUP.company_sizes, [
      ["00", "Não informado"],
      ["01", "Micro empresa"],
      ["03", "Empresa de pequeno porte"],
      ["05", "Demais"],
    ]),
    createLookupSeedSql(NOMES_TABELAS_LOOKUP.branch_types, [
      ["1", "Matriz"],
      ["2", "Filial"],
    ]),
    createLookupSeedSql(NOMES_TABELAS_LOOKUP.registration_statuses, [
      ["01", "Nula"],
      ["2", "Ativa"],
      ["3", "Suspensa"],
      ["4", "Inapta"],
      ["08", "Baixada"],
    ]),
    createLookupSeedSql(NOMES_TABELAS_LOOKUP.partner_types, [
      ["1", "Pessoa jurídica"],
      ["2", "Pessoa física"],
      ["3", "Estrangeiro"],
    ]),
    createLookupSeedSql(NOMES_TABELAS_LOOKUP.age_groups, [
      ["0", "Não se aplica"],
      ["1", "0 a 12 anos"],
      ["2", "13 a 20 anos"],
      ["3", "21 a 30 anos"],
      ["4", "31 a 40 anos"],
      ["5", "41 a 50 anos"],
      ["6", "51 a 60 anos"],
      ["7", "61 a 70 anos"],
      ["8", "71 a 80 anos"],
      ["9", "Maiores de 80 anos"],
    ]),
  ];
}
