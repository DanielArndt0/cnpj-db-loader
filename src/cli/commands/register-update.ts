import type { Command } from "commander";

import { confirm } from "../../core/prompts/confirm.js";
import { resolveInputMode } from "../../services/input-mode.service.js";

export function registerUpdateCommands(program: Command): void {
  const update = program
    .command("update")
    .description("Run monthly update workflows.");

  update
    .command("data")
    .requiredOption("--input <path>", "Path to the update directory.")
    .option("--db-url <url>", "Override the default PostgreSQL connection URL.")
    .option("--already-extracted", "Read an already extracted dataset tree.")
    .option("--unzip", "Extract the zip archives before the update flow.")
    .option(
      "--output <path>",
      'Optional extracted output directory. Defaults to "<input>/extracted" when using --unzip.',
    )
    .option("-f, --force", "Skip the confirmation prompt.")
    .description(
      "Reserved command for future dataset updates and reprocessing.",
    )
    .action(
      async (options: {
        input: string;
        dbUrl?: string;
        alreadyExtracted?: boolean;
        unzip?: boolean;
        output?: string;
        force?: boolean;
      }) => {
        const mode = resolveInputMode(options);

        if (!options.force) {
          const confirmed = await confirm(
            `Start the update workflow for ${options.input} in ${mode} mode?`,
          );
          if (!confirmed) {
            console.log("Update cancelled.");
            return;
          }
        }

        console.log(
          `Update scaffold ready. Future implementation will process ${options.input} in ${mode} mode${options.output ? ` using ${options.output} as extraction output` : ""} and use ${options.dbUrl ?? "the configured default database"}.`,
        );
      },
    );
}
