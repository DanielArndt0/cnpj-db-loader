import type { Command } from "commander";

import { inspectFiles, writeCommandLog } from "../../services/index.js";
import { printInspectSummary } from "../ui/output.js";

export function registerInspectCommands(program: Command): void {
  program
    .command("inspect")
    .argument("<input>", "Caminho do diretório de entrada.")
    .description(
      "Inspeciona arquivos e diretórios, informa os blocos de dataset reconhecidos e sugere o próximo comando.",
    )
    .action(async (input: string) => {
      const summary = await inspectFiles(input);
      const inspectLogPath = await writeCommandLog("inspect", summary);
      printInspectSummary(summary, inspectLogPath);
    });
}
