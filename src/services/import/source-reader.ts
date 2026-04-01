import { iterateFileLines } from "./planning.js";

export type ImportSourceLine = {
  rawLine: string;
  nextOffset: number;
  lineNumber: number;
};

export async function* readImportSourceLines(
  filePath: string,
  startOffset = 0,
  startLineNumber = 0,
): AsyncGenerator<ImportSourceLine> {
  let lineNumber = startLineNumber;

  for await (const item of iterateFileLines(filePath, startOffset)) {
    lineNumber += 1;
    yield {
      rawLine: item.line,
      nextOffset: item.nextOffset,
      lineNumber,
    };
  }
}
