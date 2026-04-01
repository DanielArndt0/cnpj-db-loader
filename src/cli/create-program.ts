import { Command } from "commander";

import { registerDatabaseCommands } from "./commands/register-database.js";
import { registerExtractCommands } from "./commands/register-extract.js";
import { registerDoctorCommands } from "./commands/register-doctor.js";
import { registerInspectCommands } from "./commands/register-inspect.js";
import { registerImportCommands } from "./commands/register-import.js";
import { registerQuarantineCommands } from "./commands/register-quarantine.js";
import { registerSchemaCommands } from "./commands/register-schema.js";
import { registerSanitizeCommands } from "./commands/register-sanitize.js";
import { registerValidateCommands } from "./commands/register-validate.js";
import { APP_CONFIG } from "./shared/app-config.js";
import { rootFooter } from "./shared/help.js";
import { configureProgramUi } from "./ui/output.js";

export function buildProgram(): Command {
  const program = new Command();

  configureProgramUi(program);

  program
    .name(APP_CONFIG.appName)
    .description(APP_CONFIG.description)
    .version(APP_CONFIG.version)
    .showHelpAfterError('(use "--help" for detailed usage)')
    .showSuggestionAfterError(true)
    .addHelpText("after", rootFooter());

  registerInspectCommands(program);
  registerExtractCommands(program);
  registerValidateCommands(program);
  registerSanitizeCommands(program);
  registerSchemaCommands(program);
  registerDatabaseCommands(program);
  registerImportCommands(program);
  registerQuarantineCommands(program);
  registerDoctorCommands(program);

  return program;
}
