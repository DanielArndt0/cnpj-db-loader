import type { Command } from "commander";

import { extractArchives, writeCommandLog } from "../../services/index.js";
import {
  createExtractionProgressReporter,
  printExtractionSummary,
} from "../ui/output.js";

export function registerExtractCommands(program: Command): void {
  program
    .command("extract")
    .argument(
      "<input>",
      "Path to the root directory that contains the Receita Federal ZIP archives.",
    )
    .option(
      "--output <path>",
      'Optional extracted output directory. Defaults to "<input>/extracted".',
    )
    .description(
      "Extract every ZIP archive found inside the provided input directory.",
    )
    .action(async (input: string, options: { output?: string }) => {
      const progress = createExtractionProgressReporter();
      const summary = await extractArchives(input, options.output, progress);
      const logFilePath = await writeCommandLog("extract", summary);
      printExtractionSummary(summary, logFilePath);
    });
}
