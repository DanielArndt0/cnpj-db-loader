import type { Command } from "commander";

import {
  validateInputDirectory,
  writeCommandLog,
} from "../../services/index.js";
import { printValidationSummary } from "../ui/output.js";

export function registerValidateCommands(program: Command): void {
  program
    .command("validate")
    .argument("<input>", "Caminho do diretório de entrada.")
    .description(
      "Valida se os arquivos de dataset atuais são reconhecíveis e avisa sobre blocos esperados ausentes.",
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
