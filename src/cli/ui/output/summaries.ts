import { theme } from "../theme.js";
import type { ExtractionSummary } from "../../../services/extract.service.js";
import type { InspectSummary } from "../../../services/inspect.service.js";
import type { ImportSummary } from "../../../services/import.service.js";
import type { SanitizeSummary } from "../../../services/sanitize.service.js";
import type { ValidationSummary } from "../../../services/validate.service.js";
import {
  formatBytes,
  formatCount,
  formatDuration,
  formatKeyValue,
  formatRate,
  printErrors,
  printWarnings,
  resolveLogFilePath,
} from "./shared.js";

export function printInspectSummary(
  summary: InspectSummary,
  logFilePath: string,
): void {
  console.log(theme.successLabel("INSPECT"), "Inspection completed.");
  console.log(formatKeyValue("Input path", summary.inputPath));
  console.log(formatKeyValue("Detected mode", summary.detectedInputMode));
  console.log(formatKeyValue("Total entries", summary.totalEntries));
  console.log(formatKeyValue("Zip archives", summary.zipArchivesFound));
  console.log(
    formatKeyValue(
      "Recognized extracted entries",
      summary.extractedEntriesFound,
    ),
  );

  const recognizedDatasets = Object.entries(summary.recognizedDatasets);
  if (recognizedDatasets.length > 0) {
    console.log(theme.infoLabel("DATASETS"));
    for (const [dataset, count] of recognizedDatasets) {
      console.log(`  ${theme.blue("•")} ${dataset}: ${count}`);
    }
  }

  printWarnings(summary.warnings);
  if (summary.nextStep) {
    console.log(`${theme.infoLabel("NEXT")} ${summary.nextStep}`);
  }

  console.log(`${theme.muted("Log file:")} ${resolveLogFilePath(logFilePath)}`);
}

export function printExtractionSummary(
  summary: ExtractionSummary,
  logFilePath: string,
): void {
  console.log(theme.successLabel("EXTRACT"), "Extraction completed.");
  console.log(formatKeyValue("Input path", summary.inputPath));
  console.log(formatKeyValue("Output path", summary.outputPath));
  console.log(formatKeyValue("Operating system", summary.operatingSystem));
  console.log(formatKeyValue("Zip files found", summary.zipFilesFound));
  console.log(
    formatKeyValue("Archives extracted", summary.extractedArchives.length),
  );
  console.log(formatKeyValue("Failed archives", summary.failedArchives.length));
  console.log(
    formatKeyValue(
      "Processed archive bytes",
      `${formatBytes(summary.extractedArchiveBytes)} / ${formatBytes(summary.totalArchiveBytes)}`,
    ),
  );

  printWarnings(
    summary.failedArchives.length > 0
      ? [
          "Some archives could not be extracted. Check the log file for details.",
        ]
      : [],
  );

  console.log(`${theme.muted("Log file:")} ${resolveLogFilePath(logFilePath)}`);
}

export function printValidationSummary(
  summary: ValidationSummary,
  logFilePath: string,
): void {
  console.log(
    theme.successLabel("VALIDATE"),
    summary.ok ? "Validation completed." : "Validation completed with errors.",
  );
  console.log(formatKeyValue("Input path", summary.inspected.inputPath));
  console.log(formatKeyValue("Validated path", summary.validatedPath));
  console.log(
    formatKeyValue("Detected mode", summary.inspected.detectedInputMode),
  );
  console.log(formatKeyValue("Total entries", summary.inspected.totalEntries));
  console.log(
    formatKeyValue("Recognized datasets", summary.presentDatasets.length),
  );
  console.log(
    formatKeyValue("Missing datasets", summary.missingDatasets.length),
  );
  console.log(formatKeyValue("Errors", summary.errors.length));
  console.log(formatKeyValue("Warnings", summary.warnings.length));

  if (summary.presentDatasets.length > 0) {
    console.log(theme.infoLabel("DATASETS"));
    for (const dataset of summary.presentDatasets) {
      console.log(`  ${theme.blue("•")} ${dataset}`);
    }
  }

  if (summary.missingDatasets.length > 0) {
    console.log(theme.warningLabel("MISSING"));
    for (const dataset of summary.missingDatasets) {
      console.log(`  ${theme.yellow("•")} ${dataset}`);
    }
  }

  printErrors(summary.errors);
  printWarnings(summary.warnings);
  if (summary.nextStep) {
    console.log(`${theme.infoLabel("NEXT")} ${summary.nextStep}`);
  }
  console.log(`${theme.muted("Log file:")} ${resolveLogFilePath(logFilePath)}`);
}

export function printDbConfigSummary(
  config: { defaultDbUrl?: string },
  logFilePath: string,
): void {
  console.log(theme.successLabel("DB"), "Database configuration loaded.");
  console.log(
    formatKeyValue("Default DB URL", config.defaultDbUrl ?? "not configured"),
  );
  console.log(`${theme.muted("Log file:")} ${resolveLogFilePath(logFilePath)}`);
}

export function printInfoWithLog(
  label: string,
  message: string,
  logFilePath: string,
): void {
  console.log(theme.successLabel(label), message);
  console.log(`${theme.muted("Log file:")} ${resolveLogFilePath(logFilePath)}`);
}

export function printSanitizeSummary(
  summary: SanitizeSummary,
  logFilePath: string,
): void {
  console.log(
    theme.successLabel("SANITIZE"),
    "Dataset sanitization completed.",
  );
  console.log(formatKeyValue("Input path", summary.inputPath));
  console.log(formatKeyValue("Validated path", summary.validatedPath));
  console.log(formatKeyValue("Output path", summary.outputPath));
  console.log(formatKeyValue("Processed files", summary.processedFiles));
  console.log(
    formatKeyValue("Rows counted", formatCount(summary.processedRows)),
  );
  console.log(
    formatKeyValue("Processed bytes", formatBytes(summary.totalBytes)),
  );
  console.log(
    formatKeyValue("Removed NUL bytes", formatCount(summary.nulBytesRemoved)),
  );
  console.log(formatKeyValue("Changed files", summary.changedFiles));
  console.log(formatKeyValue("Unchanged files", summary.unchangedFiles));

  if (summary.datasets.length > 0) {
    console.log(theme.infoLabel("DATASETS"));
    for (const dataset of summary.datasets) {
      console.log(`  ${theme.blue("•")} ${dataset}`);
    }
  }

  printWarnings(summary.warnings);
  if (summary.nextStep) {
    console.log(`${theme.infoLabel("NEXT")} ${summary.nextStep}`);
  }

  console.log(`${theme.muted("Log file:")} ${resolveLogFilePath(logFilePath)}`);
}

export function printImportSummary(
  summary: ImportSummary,
  logFilePath: string,
): void {
  console.log(theme.successLabel("IMPORT"), "Database import completed.");
  console.log(formatKeyValue("Input path", summary.inputPath));
  console.log(formatKeyValue("Validated path", summary.validatedPath));
  console.log(formatKeyValue("Target database", summary.targetDatabase));
  console.log(
    formatKeyValue("Imported datasets", summary.importedDatasets.length),
  );
  console.log(formatKeyValue("Imported files", summary.importedFiles));
  console.log(
    formatKeyValue("Rows committed", formatCount(summary.processedRows)),
  );
  console.log(formatKeyValue("Rows planned", formatCount(summary.plannedRows)));
  console.log(
    formatKeyValue(
      "Batches committed",
      `${formatCount(summary.committedBatches)} / ${formatCount(summary.plannedBatches)}`,
    ),
  );
  console.log(
    formatKeyValue(
      "Secondary CNAE rows",
      formatCount(summary.secondaryCnaesRows),
    ),
  );
  console.log(
    formatKeyValue("Quarantined rows", formatCount(summary.quarantinedRows)),
  );
  console.log(formatKeyValue("Resumed files", summary.resumedFiles));
  console.log(
    formatKeyValue("Checkpoint-complete files", summary.skippedCompletedFiles),
  );

  if (summary.datasetSummaries.length > 0) {
    console.log(theme.infoLabel("DATASETS"));
    for (const datasetSummary of summary.datasetSummaries) {
      console.log(
        `  ${theme.blue("•")} ${datasetSummary.dataset}: ${datasetSummary.files} file(s), ${formatCount(datasetSummary.rows)} row(s)`,
      );
    }
  }

  console.log(theme.infoLabel("PERFORMANCE"));
  console.log(
    formatKeyValue(
      "Total duration",
      formatDuration(summary.performance.totalDurationMs),
    ),
  );
  console.log(
    formatKeyValue(
      "Preparatory scan",
      formatDuration(summary.performance.scanDurationMs),
    ),
  );
  console.log(
    formatKeyValue(
      "Import execution",
      formatDuration(summary.performance.executionDurationMs),
    ),
  );
  console.log(
    formatKeyValue(
      "Lookup loading",
      formatDuration(summary.performance.lookupLoadDurationMs),
    ),
  );
  console.log(
    formatKeyValue(
      "Insert path",
      formatDuration(summary.performance.insertDurationMs),
    ),
  );
  console.log(
    formatKeyValue(
      "Retry path",
      formatDuration(summary.performance.retryDurationMs),
    ),
  );
  console.log(
    formatKeyValue(
      "Quarantine path",
      formatDuration(summary.performance.quarantineDurationMs),
    ),
  );
  console.log(
    formatKeyValue(
      "Throughput",
      `${formatRate(summary.performance.rowsPerSecond, "rows/s")} | ${formatRate(summary.performance.batchesPerMinute, "batches/min")}`,
    ),
  );

  if (summary.performance.datasets.length > 0) {
    console.log(theme.infoLabel("DATASET PERFORMANCE"));
    for (const datasetPerformance of summary.performance.datasets) {
      console.log(
        `  ${theme.blue("•")} ${datasetPerformance.dataset}: ${formatCount(datasetPerformance.importedRows)} row(s), ${formatCount(datasetPerformance.committedBatches)} batch(es), ${formatDuration(datasetPerformance.importDurationMs)}, ${formatRate(datasetPerformance.rowsPerSecond, "rows/s")}`,
      );
      console.log(
        `    scan ${formatDuration(datasetPerformance.scanDurationMs)} | insert ${formatDuration(datasetPerformance.insertDurationMs)} | retry ${formatDuration(datasetPerformance.retryDurationMs)} | quarantine ${formatDuration(datasetPerformance.quarantineDurationMs)} | resumed ${formatCount(datasetPerformance.resumedFiles)} | skipped ${formatCount(datasetPerformance.skippedCompletedFiles)}`,
      );
    }
  }

  printWarnings(summary.warnings);
  console.log(`${theme.muted("Log file:")} ${resolveLogFilePath(logFilePath)}`);
  console.log(
    `${theme.muted("Progress log:")} ${resolveLogFilePath(summary.progressLogPath)}`,
  );
}
