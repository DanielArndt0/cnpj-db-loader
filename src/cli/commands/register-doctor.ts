import type { Command } from "commander";

import { runDoctor } from "../../services/index.js";

export function registerDoctorCommands(program: Command): void {
  program
    .command("doctor")
    .option("--input <path>", "Input directory to check.")
    .option("--db-url <url>", "Override the default PostgreSQL connection URL.")
    .description("Run a basic environment doctor for the current setup.")
    .action(async (options: { input?: string; dbUrl?: string }) => {
      const report = await runDoctor(options.input, options.dbUrl);
      console.log(report.join("\n"));
    });
}
