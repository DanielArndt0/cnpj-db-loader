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

  console.log(theme.infoLabel("FILTERS"));
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
    "Quarantine statistics loaded.",
  );
  console.log(formatKeyValue("Total rows", formatCount(summary.totalRows)));
  console.log(
    formatKeyValue("Retryable rows", formatCount(summary.retryableRows)),
  );
  console.log(
    formatKeyValue("Terminal rows", formatCount(summary.terminalRows)),
  );

  printAppliedFilters(summary.appliedFilters);

  if (summary.rowsByDataset.length > 0) {
    console.log(theme.infoLabel("BY DATASET"));
    for (const item of summary.rowsByDataset) {
      console.log(
        `  ${theme.blue("•")} ${item.key}: ${formatCount(item.count)}`,
      );
    }
  }

  if (summary.rowsByCategory.length > 0) {
    console.log(theme.infoLabel("BY CATEGORY"));
    for (const item of summary.rowsByCategory) {
      console.log(
        `  ${theme.blue("•")} ${item.key}: ${formatCount(item.count)}`,
      );
    }
  }

  if (summary.rowsByStage.length > 0) {
    console.log(theme.infoLabel("BY STAGE"));
    for (const item of summary.rowsByStage) {
      console.log(
        `  ${theme.blue("•")} ${item.key}: ${formatCount(item.count)}`,
      );
    }
  }

  console.log(`${theme.muted("Log file:")} ${resolveLogFilePath(logFilePath)}`);
}

export function printQuarantineListSummary(
  summary: QuarantineListSummary,
  logFilePath: string,
): void {
  console.log(theme.successLabel("QUARANTINE"), "Quarantine rows loaded.");
  console.log(
    formatKeyValue("Returned rows", formatCount(summary.rows.length)),
  );
  console.log(formatKeyValue("Limit", summary.appliedFilters.limit));

  printAppliedFilters(summary.appliedFilters);

  if (summary.rows.length > 0) {
    console.log(theme.infoLabel("ROWS"));
    for (const row of summary.rows) {
      const retryLabel = row.canRetryLater ? "retryable" : "terminal";
      console.log(
        `  ${theme.blue("•")} #${row.id} | ${row.dataset} | ${row.errorCategory ?? "unknown"} | ${retryLabel}`,
      );
      console.log(
        `    ${truncateMiddle(row.filePath, 88)} | row ${row.rowNumber ?? "?"} | offset ${
          row.checkpointOffset === null
            ? "?"
            : formatBytes(row.checkpointOffset)
        }`,
      );
      console.log(`    ${truncateMiddle(row.errorMessage, 110)}`);
    }
  }

  console.log(`${theme.muted("Log file:")} ${resolveLogFilePath(logFilePath)}`);
}

export function printQuarantineRecord(
  record: QuarantineRecord,
  logFilePath: string,
): void {
  console.log(
    theme.successLabel("QUARANTINE"),
    `Quarantine row #${record.id} loaded.`,
  );
  console.log(formatKeyValue("Dataset", record.dataset));
  console.log(formatKeyValue("File path", record.filePath));
  console.log(
    formatKeyValue("Row number", record.rowNumber ?? "not available"),
  );
  console.log(
    formatKeyValue(
      "Checkpoint offset",
      record.checkpointOffset === null
        ? "not available"
        : formatBytes(record.checkpointOffset),
    ),
  );
  console.log(formatKeyValue("Error code", record.errorCode ?? "unknown"));
  console.log(
    formatKeyValue("Error category", record.errorCategory ?? "unknown"),
  );
  console.log(formatKeyValue("Error stage", record.errorStage ?? "unknown"));
  console.log(formatKeyValue("Retry count", record.retryCount));
  console.log(formatKeyValue("Retryable", record.canRetryLater ? "yes" : "no"));
  console.log(formatKeyValue("Created at", record.createdAt));

  console.log(theme.infoLabel("ERROR"));
  console.log(`  ${record.errorMessage}`);

  console.log(theme.infoLabel("RAW LINE"));
  console.log(`  ${record.rawLine}`);

  if (record.sanitizationsApplied.length > 0) {
    console.log(theme.infoLabel("SANITIZATIONS"));
    for (const item of record.sanitizationsApplied) {
      console.log(`  ${theme.blue("•")} ${String(item)}`);
    }
  }

  if (record.parsedPayload) {
    console.log(theme.infoLabel("PARSED PAYLOAD"));
    console.log(JSON.stringify(record.parsedPayload, null, 2));
  }

  console.log(`${theme.muted("Log file:")} ${resolveLogFilePath(logFilePath)}`);
}
