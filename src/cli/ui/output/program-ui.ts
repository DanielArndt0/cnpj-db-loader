import type { Command, Option } from "commander";

import { theme } from "../theme.js";

export function configureProgramUi(program: Command): void {
  program.configureOutput({
    writeErr: (str) => process.stderr.write(str),
    outputError: (str, write) => {
      write(theme.red(str));
    },
  });

  program.configureHelp({
    subcommandTerm: (cmd) => theme.command(cmd.name()),
    optionTerm: (option: Option) => theme.flag(option.flags),
  });
}
