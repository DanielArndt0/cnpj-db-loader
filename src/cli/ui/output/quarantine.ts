import type {
  QuarantineListSummary,
  QuarantineRecord,
  QuarantineStatsSummary,
} from "../../../services/quarantine.service.js";
import { theme } from "../theme.js";
import {
  formatBytes,
  formatCount,
  formatKeyValue,
  resolveLogFilePath,
  truncateMiddle,
} from "./shared.js";

function printAppliedFilters(summaryFilters: Record<string, unknown>): void {
  const activeFilters = Object.entries(summaryFilters).filter(([, value]) =>
    typeof value === "boolean" ? value : value !== undefined,
  );

  if (activeFilters.length === 0) {
    return;
  }

  console.log(theme.infoLabel("FILTROS"));
  for (const [key, value] of activeFilters) {
    console.log(`  ${theme.blue("•")} ${key}: ${String(value)}`);
  }
}

export function printQuarantineStatsSummary(
  summary: QuarantineStatsSummary,
  logFilePath: string,
): void {
  console.log(
    theme.successLabel("QUARANTINE"),
    "Estatísticas de quarentena carregadas.",
  );
  console.log(
    formatKeyValue("Total de linhas", formatCount(summary.totalRows)),
  );
  console.log(
    formatKeyValue("Linhas reprocessáveis", formatCount(summary.retryableRows)),
  );
  console.log(
    formatKeyValue("Linhas terminais", formatCount(summary.terminalRows)),
  );

  printAppliedFilters(summary.appliedFilters);

  if (summary.rowsByDataset.length > 0) {
    console.log(theme.infoLabel("POR CONJUNTO"));
    for (const item of summary.rowsByDataset) {
      console.log(
        `  ${theme.blue("•")} ${item.key}: ${formatCount(item.count)}`,
      );
    }
  }

  if (summary.rowsByCategory.length > 0) {
    console.log(theme.infoLabel("POR CATEGORIA"));
    for (const item of summary.rowsByCategory) {
      console.log(
        `  ${theme.blue("•")} ${item.key}: ${formatCount(item.count)}`,
      );
    }
  }

  if (summary.rowsByStage.length > 0) {
    console.log(theme.infoLabel("POR ETAPA"));
    for (const item of summary.rowsByStage) {
      console.log(
        `  ${theme.blue("•")} ${item.key}: ${formatCount(item.count)}`,
      );
    }
  }

  console.log(
    `${theme.muted("Arquivo de log:")} ${resolveLogFilePath(logFilePath)}`,
  );
}

export function printQuarantineListSummary(
  summary: QuarantineListSummary,
  logFilePath: string,
): void {
  console.log(
    theme.successLabel("QUARANTINE"),
    "Linhas de quarentena carregadas.",
  );
  console.log(
    formatKeyValue("Linhas retornadas", formatCount(summary.rows.length)),
  );
  console.log(formatKeyValue("Limite", summary.appliedFilters.limit));

  printAppliedFilters(summary.appliedFilters);

  if (summary.rows.length > 0) {
    console.log(theme.infoLabel("LINHAS"));
    for (const row of summary.rows) {
      const retryLabel = row.canRetryLater ? "reprocessável" : "terminal";
      console.log(
        `  ${theme.blue("•")} #${row.id} | ${row.dataset} | ${row.errorCategory ?? "desconhecida"} | ${retryLabel}`,
      );
      console.log(
        `    ${truncateMiddle(row.filePath, 88)} | linha ${row.rowNumber ?? "?"} | offset ${
          row.checkpointOffset === null
            ? "?"
            : formatBytes(row.checkpointOffset)
        }`,
      );
      console.log(`    ${truncateMiddle(row.errorMessage, 110)}`);
    }
  }

  console.log(
    `${theme.muted("Arquivo de log:")} ${resolveLogFilePath(logFilePath)}`,
  );
}

export function printQuarantineRecord(
  record: QuarantineRecord,
  logFilePath: string,
): void {
  console.log(
    theme.successLabel("QUARANTINE"),
    `Linha de quarentena #${record.id} carregada.`,
  );
  console.log(formatKeyValue("Conjunto", record.dataset));
  console.log(formatKeyValue("Caminho do arquivo", record.filePath));
  console.log(
    formatKeyValue("Número da linha", record.rowNumber ?? "não disponível"),
  );
  console.log(
    formatKeyValue(
      "Deslocamento do checkpoint",
      record.checkpointOffset === null
        ? "não disponível"
        : formatBytes(record.checkpointOffset),
    ),
  );
  console.log(
    formatKeyValue("Código do erro", record.errorCode ?? "desconhecido"),
  );
  console.log(
    formatKeyValue("Categoria do erro", record.errorCategory ?? "desconhecida"),
  );
  console.log(
    formatKeyValue("Etapa do erro", record.errorStage ?? "desconhecida"),
  );
  console.log(formatKeyValue("Total de tentativas", record.retryCount));
  console.log(
    formatKeyValue("Reprocessável", record.canRetryLater ? "sim" : "não"),
  );
  console.log(formatKeyValue("Criado em", record.createdAt));

  console.log(theme.infoLabel("ERRO"));
  console.log(`  ${record.errorMessage}`);

  console.log(theme.infoLabel("LINHA BRUTA"));
  console.log(`  ${record.rawLine}`);

  if (record.sanitizationsApplied.length > 0) {
    console.log(theme.infoLabel("SANITIZAÇÕES"));
    for (const item of record.sanitizationsApplied) {
      console.log(`  ${theme.blue("•")} ${String(item)}`);
    }
  }

  if (record.parsedPayload) {
    console.log(theme.infoLabel("PAYLOAD PARSEADO"));
    console.log(JSON.stringify(record.parsedPayload, null, 2));
  }

  console.log(
    `${theme.muted("Arquivo de log:")} ${resolveLogFilePath(logFilePath)}`,
  );
}
