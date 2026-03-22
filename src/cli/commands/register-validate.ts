import type { Command } from "commander";

import {
  validateInputDirectory,
  writeCommandLog,
} from "../../services/index.js";
import { printValidationSummary } from "../ui/output.js";

export function registerValidateCommands(program: Command): void {
  program
    .command("validate")
    .argument("<input>", "Path to the input directory.")
    .description(
      "Validate that the current dataset files are recognizable and warn about missing expected blocks.",
    )
    .action(async (input: string) => {
      const summary = await validateInputDirectory(input);
      const logFilePath = await writeCommandLog("validate", summary);
      printValidationSummary(summary, logFilePath);

      if (!summary.ok) {
        process.exitCode = 1;
      }
    });
}
