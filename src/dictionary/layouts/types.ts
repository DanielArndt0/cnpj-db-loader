export type FieldDefinition = {
  sourceLabel: string;
  columnName: string;
  dataType: "text" | "integer" | "numeric" | "date" | "boolean";
  nullable?: boolean;
  notes?: string;
};

export type TableLayout = {
  key: string;
  tableName: string;
  sourceName: string;
  description: string;
  fields: FieldDefinition[];
};
