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
      "Caminho do diretório raiz que contém os arquivos ZIP da Receita Federal.",
    )
    .option(
      "--output <path>",
      'Diretório de saída da extração (opcional). Padrão: "<input>/extracted".',
    )
    .description(
      "Extrai todos os arquivos ZIP encontrados dentro do diretório de entrada informado.",
    )
    .action(async (input: string, options: { output?: string }) => {
      const progress = createExtractionProgressReporter();
      const summary = await extractArchives(input, options.output, progress);
      const logFilePath = await writeCommandLog("extract", summary);
      printExtractionSummary(summary, logFilePath);
    });
}
