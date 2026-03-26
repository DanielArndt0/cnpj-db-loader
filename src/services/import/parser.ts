import { parseDelimitedLine } from "./transform.js";
import type { ImportSourceLine } from "./source-reader.js";

export type ParsedImportSourceLine = ImportSourceLine & {
  fields: string[];
};

export function parseImportSourceLine(
  sourceLine: ImportSourceLine,
): ParsedImportSourceLine {
  return {
    ...sourceLine,
    fields: parseDelimitedLine(sourceLine.rawLine),
  };
}
