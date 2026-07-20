import { access } from "node:fs/promises";

import {
  resolveDatabaseUrl,
  testDatabaseConnection,
} from "./database.service.js";

export async function runDoctor(
  inputPath?: string,
  dbUrl?: string,
): Promise<string[]> {
  const report: string[] = [];

  if (inputPath) {
    try {
      await access(inputPath);
      report.push(`Caminho de entrada acessível: ${inputPath}`);
    } catch {
      report.push(`Caminho de entrada inacessível: ${inputPath}`);
    }
  }

  try {
    const resolvedDbUrl = await resolveDatabaseUrl(dbUrl);
    await testDatabaseConnection(resolvedDbUrl);
    report.push("Conexão com o banco bem-sucedida.");
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    report.push(`Verificação do banco falhou: ${message}`);
  }

  return report;
}
