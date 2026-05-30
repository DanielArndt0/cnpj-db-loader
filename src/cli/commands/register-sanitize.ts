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
    .option(
      "--source-encoding <encoding>",
      "Source file encoding used while reading Receita files. Defaults to LATIN1 (ISO-8859-1) and writes validated UTF-8 output.",
    )
    .option(
      "--allow-replacement-chars",
      "Allow Unicode replacement characters in sanitized output instead of failing validation.",
    )
    .option("-f, --force", "Skip the confirmation prompt.")
    .description(
      "Normalize Receita source files into validated UTF-8 output before import.",
    )
    .action(
      async (
        input: string,
        options: {
          output?: string;
          dataset?: string;
          sourceEncoding?: string;
          allowReplacementChars?: boolean;
          force?: boolean;
        },
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

        if (options.sourceEncoding) {
          sanitizeOptions.sourceEncoding = options.sourceEncoding;
        }

        if (options.allowReplacementChars) {
          sanitizeOptions.strict = false;
        }

        const summary = await sanitizeInputDirectory(input, sanitizeOptions);
        const logFilePath = await writeCommandLog("sanitize", summary);
        printSanitizeSummary(summary, logFilePath);
      },
    );
}
