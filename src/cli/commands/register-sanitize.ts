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
    .argument("<input>", "Caminho do diretório do dataset extraído e validado.")
    .option(
      "--output <path>",
      "Diretório de saída personalizado para a árvore de dataset sanitizado.",
    )
    .option(
      "--dataset <dataset>",
      "Sanitiza apenas um bloco de dataset validado (por exemplo: establishments ou companies).",
    )
    .option(
      "--source-encoding <encoding>",
      "Encoding dos arquivos de origem usado ao ler os arquivos da Receita. Padrão: LATIN1 (ISO-8859-1); grava saída UTF-8 validada.",
    )
    .option(
      "--allow-replacement-chars",
      "Permite caracteres de substituição Unicode na saída sanitizada em vez de falhar a validação.",
    )
    .option("-f, --force", "Pula a confirmação interativa.")
    .description(
      "Normaliza os arquivos de origem da Receita em saída UTF-8 validada antes da importação.",
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
            `Preparar agora uma árvore de dataset sanitizado a partir de ${input}? Este comando cria uma nova árvore de saída para importações mais rápidas.`,
          );
          if (!confirmed) {
            console.log("Sanitização cancelada.");
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
