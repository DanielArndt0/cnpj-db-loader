import { AppError } from "../../../core/errors/index.js";
import { theme } from "../theme.js";

export function handleCliError(error: unknown): never {
  if (error instanceof AppError) {
    console.error(`${theme.errorLabel(error.code)} ${error.message}`);
    process.exit(1);
  }

  const message = error instanceof Error ? error.message : String(error);
  console.error(`${theme.errorLabel("UNEXPECTED_ERROR")} ${message}`);
  process.exit(1);
}
