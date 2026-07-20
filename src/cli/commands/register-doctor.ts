import type { Command } from "commander";

import { runDoctor } from "../../services/index.js";

export function registerDoctorCommands(program: Command): void {
  program
    .command("doctor")
    .option("--input <path>", "Diretório de entrada a verificar.")
    .option("--db-url <url>", "Sobrescreve a URL de conexão PostgreSQL padrão.")
    .description(
      "Executa um diagnóstico básico do ambiente para a configuração atual.",
    )
    .action(async (options: { input?: string; dbUrl?: string }) => {
      const report = await runDoctor(options.input, options.dbUrl);
      console.log(report.join("\n"));
    });
}
