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
  return ["-- Domain tables", ...domainTables.map(createSimpleDomainTableSql)];
}

export function createDomainSeedParts(): string[] {
  return [
    "-- Domain seed data",
    createLookupSeedSql("company_sizes", [
      ["00", "Not informed"],
      ["01", "Micro company"],
      ["03", "Small business"],
      ["05", "Other"],
    ]),
    createLookupSeedSql("branch_types", [
      ["1", "Headquarters"],
      ["2", "Branch"],
    ]),
    createLookupSeedSql("registration_statuses", [
      ["01", "Null"],
      ["2", "Active"],
      ["3", "Suspended"],
      ["4", "Inactive"],
      ["08", "Closed"],
    ]),
    createLookupSeedSql("partner_types", [
      ["1", "Legal entity"],
      ["2", "Natural person"],
      ["3", "Foreign person/entity"],
    ]),
    createLookupSeedSql("age_groups", [
      ["0", "Not applicable"],
      ["1", "0 to 12 years"],
      ["2", "13 to 20 years"],
      ["3", "21 to 30 years"],
      ["4", "31 to 40 years"],
      ["5", "41 to 50 years"],
      ["6", "51 to 60 years"],
      ["7", "61 to 70 years"],
      ["8", "71 to 80 years"],
      ["9", "Over 80 years"],
    ]),
  ];
}
