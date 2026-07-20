import type { Command } from "commander";

import { prettyJson } from "../../core/utils/index.js";
import { getAllLayouts, getLayoutSummary } from "../../services/index.js";

export function registerModelCommands(program: Command): void {
  const model = program
    .command("model")
    .description("Inspeciona o modelo de dados interno.");

  model
    .command("show")
    .description("Imprime o dicionário completo de layouts internos.")
    .action(() => {
      console.log(prettyJson(getAllLayouts()));
    });

  model
    .command("summary")
    .description("Imprime um resumo compacto dos blocos de dataset.")
    .action(() => {
      console.log(prettyJson(getLayoutSummary()));
    });
}
