import type {
  FieldDefinition,
  TableLayout,
} from "../../dictionary/layouts/index.js";

export function mapType(dataType: FieldDefinition["dataType"]): string {
  switch (dataType) {
    case "integer":
      return "integer";
    case "numeric":
      return "numeric(18,2)";
    case "date":
      return "date";
    case "boolean":
      return "boolean";
    default:
      return "text";
  }
}

export function createColumnSql(field: FieldDefinition): string {
  return `  ${field.columnName} ${mapType(field.dataType)}${field.nullable ? "" : " not null"}`;
}

export function createSimpleDomainTableSql(layout: TableLayout): string {
  const columns = layout.fields.map(createColumnSql).join(",\n");
  return [
    `create table if not exists ${layout.tableName} (`,
    columns + ",",
    "  primary key (codigo)",
    ");",
  ].join("\n");
}

export function createLookupSeedSql(
  tableName: string,
  rows: Array<[string, string]>,
): string {
  const values = rows
    .map(
      ([codigo, descricao]) =>
        `  ('${codigo.replace(/'/g, "''")}', '${descricao.replace(/'/g, "''")}')`,
    )
    .join(",\n");

  return [
    `insert into ${tableName} (codigo, descricao) values`,
    values,
    "on conflict (codigo) do update set descricao = excluded.descricao;",
  ].join("\n");
}
