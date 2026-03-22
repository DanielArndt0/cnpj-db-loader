import {
  ageGroupsLayout,
  branchTypesLayout,
  cnaesLayout,
  citiesLayout,
  companiesLayout,
  companySizesLayout,
  countriesLayout,
  establishmentsLayout,
  legalNaturesLayout,
  partnerQualificationsLayout,
  partnerTypesLayout,
  partnersLayout,
  reasonsLayout,
  registrationStatusesLayout,
  simplesLayout,
  type TableLayout,
} from "../dictionary/layouts/index.js";

const layouts: TableLayout[] = [
  companiesLayout,
  establishmentsLayout,
  partnersLayout,
  simplesLayout,
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

export function getAllLayouts(): TableLayout[] {
  return layouts;
}

export function getLayoutSummary(): Array<
  Pick<TableLayout, "key" | "tableName" | "sourceName">
> {
  return layouts.map(({ key, tableName, sourceName }) => ({
    key,
    tableName,
    sourceName,
  }));
}
