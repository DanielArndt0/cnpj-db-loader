import type { Command } from "commander";

import { inspectFiles, writeCommandLog } from "../../services/index.js";
import { printInspectSummary } from "../ui/output.js";

export function registerInspectCommands(program: Command): void {
  program
    .command("inspect")
    .argument("<input>", "Path to the input directory.")
    .description(
      "Inspect files and directories, report recognized dataset blocks, and suggest the next command.",
    )
    .action(async (input: string) => {
      const summary = await inspectFiles(input);
      const inspectLogPath = await writeCommandLog("inspect", summary);
      printInspectSummary(summary, inspectLogPath);
    });
}
