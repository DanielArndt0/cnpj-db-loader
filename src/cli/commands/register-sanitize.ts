import type { Command } from "commander";

import { confirm } from "../../core/prompts/confirm.js";
import type { SanitizeOptions } from "../../services/sanitize.service.js";
import {
  sanitizeInputDirectory,
  writeCommandLog,
} from "../../services/index.js";
import {
  createSanitizeProgressReporter,
  printSanitizeSummary,
} from "../ui/output.js";

export function registerSanitizeCommands(program: Command): void {
  program
    .command("sanitize")
    .argument("<input>", "Path to the validated extracted dataset directory.")
    .option(
      "--output <path>",
      "Custom output directory for the sanitized dataset tree.",
    )
    .option(
      "--dataset <dataset>",
      "Sanitize only one validated dataset block (for example: establishments or companies).",
    )
    .option("-f, --force", "Skip the confirmation prompt.")
    .description(
      "Prepare a sanitized dataset tree before import by removing known low-level byte issues such as NUL bytes.",
    )
    .action(
      async (
        input: string,
        options: { output?: string; dataset?: string; force?: boolean },
      ) => {
        if (!options.force) {
          const confirmed = await confirm(
            `Prepare a sanitized dataset tree from ${input} now? This command creates a new output tree for faster imports.`,
          );
          if (!confirmed) {
            console.log("Sanitization cancelled.");
            return;
          }
        }

        const progress = createSanitizeProgressReporter();
        const sanitizeOptions: SanitizeOptions = {
          onProgress: progress,
        };

        if (options.output) {
          sanitizeOptions.outputPath = options.output;
        }

        if (options.dataset) {
          sanitizeOptions.dataset =
            options.dataset as SanitizeOptions["dataset"];
        }

        const summary = await sanitizeInputDirectory(input, sanitizeOptions);
        const logFilePath = await writeCommandLog("sanitize", summary);
        printSanitizeSummary(summary, logFilePath);
      },
    );
}
