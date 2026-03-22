import path from "node:path";

export function defaultExtractedOutputPath(inputPath: string): string {
  return path.join(inputPath, "extracted");
}
