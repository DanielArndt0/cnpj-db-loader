import { Query, escapeIdentifier, type Client } from "pg";

function escapeCopyTextValue(value: unknown): string {
  if (value == null) {
    return "\\N";
  }

  const text =
    typeof value === "boolean"
      ? value
        ? "t"
        : "f"
      : value instanceof Date
        ? value.toISOString()
        : String(value);

  return text
    .replace(/\\/g, "\\\\")
    .replace(/\t/g, "\\t")
    .replace(/\n/g, "\\n")
    .replace(/\r/g, "\\r")
    .replace(/\f/g, "\\f")
    .split(String.fromCharCode(8))
    .join("\\b")
    .replace(/\v/g, "\\v");
}

function serializeCopyRows(rows: readonly unknown[][]): Buffer {
  const body = rows
    .map((row) => row.map((value) => escapeCopyTextValue(value)).join("\t"))
    .join("\n");

  return Buffer.from(`${body}\n`, "utf8");
}

function buildCopyStatement(
  tableName: string,
  columns: readonly string[],
): string {
  const escapedColumns = columns.map((column) => escapeIdentifier(column));
  return `copy ${escapeIdentifier(tableName)} (${escapedColumns.join(", ")}) from stdin with (format text, null '\\N')`;
}

class CopyFromTextQuery extends Query {
  private readonly payload: Buffer;

  constructor(
    text: string,
    payload: Buffer,
    callback: (error?: Error) => void,
  ) {
    super(text, undefined, callback);
    this.payload = payload;
  }

  handleCopyInResponse(connection: {
    sendCopyFromChunk: (chunk: Buffer) => void;
    endCopyFrom: () => void;
    sendCopyFail: (message: string) => void;
  }): void {
    try {
      if (this.payload.length > 0) {
        connection.sendCopyFromChunk(this.payload);
      }
      connection.endCopyFrom();
    } catch (error) {
      connection.sendCopyFail(
        error instanceof Error ? error.message : String(error),
      );
    }
  }
}

export async function copyRowsToTable(
  client: Client,
  tableName: string,
  columns: readonly string[],
  rows: readonly unknown[][],
): Promise<void> {
  if (rows.length === 0) {
    return;
  }

  const statement = buildCopyStatement(tableName, columns);
  const payload = serializeCopyRows(rows);

  await new Promise<void>((resolve, reject) => {
    const query = new CopyFromTextQuery(statement, payload, (error) => {
      if (error) {
        reject(error);
        return;
      }

      resolve();
    });

    client.query(query);
  });
}
