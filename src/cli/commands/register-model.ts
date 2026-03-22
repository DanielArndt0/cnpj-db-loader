import type { Command } from "commander";

import { prettyJson } from "../../core/utils/index.js";
import { getAllLayouts, getLayoutSummary } from "../../services/index.js";

export function registerModelCommands(program: Command): void {
  const model = program
    .command("model")
    .description("Inspect the internal data model.");

  model
    .command("show")
    .description("Print the full internal layout dictionary.")
    .action(() => {
      console.log(prettyJson(getAllLayouts()));
    });

  model
    .command("summary")
    .description("Print a compact summary of dataset blocks.")
    .action(() => {
      console.log(prettyJson(getLayoutSummary()));
    });
}
