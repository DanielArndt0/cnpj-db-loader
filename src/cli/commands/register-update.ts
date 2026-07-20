import type { Command } from "commander";

import { confirm } from "../../core/prompts/confirm.js";
import { resolveInputMode } from "../../services/input-mode.service.js";

export function registerUpdateCommands(program: Command): void {
  const update = program
    .command("update")
    .description("Executa fluxos de atualização mensal.");

  update
    .command("data")
    .requiredOption("--input <path>", "Caminho do diretório de atualização.")
    .option("--db-url <url>", "Sobrescreve a URL de conexão PostgreSQL padrão.")
    .option("--already-extracted", "Lê uma árvore de dataset já extraída.")
    .option("--unzip", "Extrai os arquivos zip antes do fluxo de atualização.")
    .option(
      "--output <path>",
      'Diretório de saída da extração (opcional). Padrão: "<input>/extracted" ao usar --unzip.',
    )
    .option("-f, --force", "Pula a confirmação interativa.")
    .description(
      "Comando reservado para futuras atualizações e reprocessamentos de dataset.",
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
            `Iniciar o fluxo de atualização para ${options.input} no modo ${mode}?`,
          );
          if (!confirmed) {
            console.log("Atualização cancelada.");
            return;
          }
        }

        console.log(
          `Estrutura de atualização pronta. A implementação futura processará ${options.input} no modo ${mode}${options.output ? ` usando ${options.output} como saída de extração` : ""} e usará ${options.dbUrl ?? "o banco padrão configurado"}.`,
        );
      },
    );
}
