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
      report.push(`Input path reachable: ${inputPath}`);
    } catch {
      report.push(`Input path not reachable: ${inputPath}`);
    }
  }

  try {
    const resolvedDbUrl = await resolveDatabaseUrl(dbUrl);
    await testDatabaseConnection(resolvedDbUrl);
    report.push("Database connection succeeded.");
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    report.push(`Database check failed: ${message}`);
  }

  return report;
}
