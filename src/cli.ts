import { buildProgram } from "./cli/create-program.js";
import { handleCliError } from "./cli/ui/output.js";

buildProgram()
  .parseAsync(process.argv)
  .catch((error: unknown) => {
    handleCliError(error);
  });
