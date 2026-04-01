#!/usr/bin/env node
import { buildProgram } from "./cli/create-program.js";
import { resolveInvokedCommandLabel } from "./cli/shared/command-label.js";
import { handleCliError } from "./cli/ui/output.js";
import { writeCommandFailureLog } from "./services/logging.service.js";

buildProgram()
  .parseAsync(process.argv)
  .catch(async (error: unknown) => {
    const argv = process.argv.slice(2);
    const commandLabel = resolveInvokedCommandLabel(argv);

    await writeCommandFailureLog(commandLabel, error, {
      argv,
      fatal: !(error instanceof Error),
    }).catch(() => undefined);

    handleCliError(error);
  });
