import type { Command, Option } from "commander";
import path from "node:path";

import { AppError } from "../../core/errors/index.js";
import type {
  ExtractionProgressEvent,
  ExtractionSummary,
} from "../../services/extract.service.js";
import type { InspectSummary } from "../../services/inspect.service.js";
import type { ValidationSummary } from "../../services/validate.service.js";
import type {
  ImportProgressEvent,
  ImportSummary,
} from "../../services/import.service.js";
import { theme } from "./theme.js";

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

function formatKeyValue(label: string, value: string | number): string {
  return `${theme.muted(`- ${label}:`)} ${value}`;
}

function printWarnings(warnings: string[]): void {
  if (warnings.length === 0) {
    return;
  }

  console.log(theme.warningLabel("WARNINGS"));
  for (const warning of warnings) {
    console.log(`  ${theme.yellow("•")} ${warning}`);
  }
}

function printErrors(errors: string[]): void {
  if (errors.length === 0) {
    return;
  }

  console.log(theme.errorLabel("ERRORS"));
  for (const error of errors) {
    console.log(`  ${theme.red("•")} ${error}`);
  }
}

function formatBytes(value: number): string {
  if (value < 1024) {
    return `${value} B`;
  }

  const units = ["KB", "MB", "GB", "TB"];
  let currentValue = value / 1024;
  let unitIndex = 0;

  while (currentValue >= 1024 && unitIndex < units.length - 1) {
    currentValue /= 1024;
    unitIndex += 1;
  }

  return `${currentValue.toFixed(currentValue >= 100 ? 0 : currentValue >= 10 ? 1 : 2)} ${units[unitIndex]}`;
}

function formatCount(value: number): string {
  return new Intl.NumberFormat("en-US").format(value);
}

function truncateMiddle(value: string, maxLength: number): string {
  if (value.length <= maxLength) {
    return value;
  }

  const edgeLength = Math.max(6, Math.floor((maxLength - 3) / 2));
  return `${value.slice(0, edgeLength)}...${value.slice(value.length - edgeLength)}`;
}

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

  console.log(`${theme.muted("Log file:")} ${path.resolve(logFilePath)}`);
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

  console.log(`${theme.muted("Log file:")} ${path.resolve(logFilePath)}`);
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
  console.log(`${theme.muted("Log file:")} ${path.resolve(logFilePath)}`);
}

export function printDbConfigSummary(
  config: { defaultDbUrl?: string },
  logFilePath: string,
): void {
  console.log(theme.successLabel("DB"), "Database configuration loaded.");
  console.log(
    formatKeyValue("Default DB URL", config.defaultDbUrl ?? "not configured"),
  );
  console.log(`${theme.muted("Log file:")} ${path.resolve(logFilePath)}`);
}

export function printInfoWithLog(
  label: string,
  message: string,
  logFilePath: string,
): void {
  console.log(theme.successLabel(label), message);
  console.log(`${theme.muted("Log file:")} ${path.resolve(logFilePath)}`);
}

export function createExtractionProgressReporter(): (
  event: ExtractionProgressEvent,
) => void {
  const frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
  let frameIndex = 0;
  let spinnerTimer: NodeJS.Timeout | undefined;
  let lastRenderedLine = "";
  let currentEvent: ExtractionProgressEvent | undefined;
  let currentArchiveName = "";
  let usedDynamicLine = false;
  let lastStableLine = "";

  const renderLine = (line: string): void => {
    if (!process.stdout.isTTY) {
      if (line !== lastRenderedLine) {
        console.log(line);
        lastRenderedLine = line;
      }
      return;
    }

    usedDynamicLine = true;

    const width = process.stdout.columns || 140;
    const paddedLine = line.padEnd(width);

    if (paddedLine === lastRenderedLine) {
      return;
    }

    process.stdout.write(`\r${paddedLine}`);
    lastRenderedLine = paddedLine;
  };

  const renderFromState = (): void => {
    if (
      !currentEvent ||
      currentEvent.kind === "start" ||
      currentEvent.kind === "finish"
    ) {
      return;
    }

    const completedArchives = currentEvent.completedArchives;
    const totalArchives = currentEvent.totalArchives;
    const remainingArchives = Math.max(
      totalArchives -
        completedArchives -
        (currentEvent.kind === "archive-start" ? 1 : 0),
      0,
    );
    const percentage =
      totalArchives === 0
        ? 100
        : Math.floor((completedArchives / totalArchives) * 100);
    const bytesProgress =
      currentEvent.totalBytes === 0
        ? "0 B / 0 B"
        : `${formatBytes(currentEvent.extractedBytes)} / ${formatBytes(currentEvent.totalBytes)}`;
    const archiveLabel = truncateMiddle(currentArchiveName, 56);

    lastStableLine =
      `${theme.infoLabel("EXTRACT")} __SPINNER__ ${percentage}% ` +
      `| ${completedArchives}/${totalArchives} archives ` +
      `| remaining ${remainingArchives} ` +
      `| ${bytesProgress} ` +
      `| current ${archiveLabel}`;

    const spinner = frames[frameIndex % frames.length] ?? "⠋";
    renderLine(lastStableLine.replace("__SPINNER__", theme.blue(spinner)));
  };

  const startSpinner = (): void => {
    if (spinnerTimer) {
      clearInterval(spinnerTimer);
    }

    spinnerTimer = setInterval(() => {
      if (!lastStableLine) {
        return;
      }

      frameIndex += 1;
      const spinner = frames[frameIndex % frames.length] ?? "⠋";
      renderLine(lastStableLine.replace("__SPINNER__", theme.blue(spinner)));
    }, 220);
  };

  const stopSpinner = (): void => {
    if (spinnerTimer) {
      clearInterval(spinnerTimer);
      spinnerTimer = undefined;
    }
  };

  const finalizeDynamicLine = (): void => {
    stopSpinner();
    if (process.stdout.isTTY && usedDynamicLine) {
      process.stdout.write("\n");
    }
  };

  return (event: ExtractionProgressEvent): void => {
    currentEvent = event;

    if (event.kind === "start") {
      console.log(theme.infoLabel("EXTRACT"), "Starting archive extraction...");
      console.log(formatKeyValue("Input path", event.inputPath));
      console.log(formatKeyValue("Output path", event.outputPath));
      console.log(formatKeyValue("Archives queued", event.totalArchives));
      console.log(
        formatKeyValue("Archive bytes", formatBytes(event.totalBytes)),
      );

      lastRenderedLine = "";
      lastStableLine = "";
      frameIndex = 0;

      return;
    }

    if (event.kind === "archive-start") {
      currentArchiveName = event.currentArchiveName;
      renderFromState();
      startSpinner();
      return;
    }

    if (event.kind === "archive-complete") {
      currentArchiveName = event.currentArchiveName;
      renderFromState();
      return;
    }

    if (event.kind === "archive-failed") {
      currentArchiveName = event.currentArchiveName;
      renderFromState();
      finalizeDynamicLine();
      console.log(
        `${theme.warningLabel("WARNING")} Failed to extract ${event.currentArchiveName}: ${event.errorMessage}`,
      );
      return;
    }

    if (event.kind === "finish") {
      finalizeDynamicLine();
      console.log(
        theme.successLabel("EXTRACT"),
        `Processed ${event.completedArchives}/${event.totalArchives} archives (${event.failedArchives} failed).`,
      );
      console.log(formatKeyValue("Output path", event.outputPath));
      console.log(
        formatKeyValue(
          "Archive bytes",
          `${formatBytes(event.extractedBytes)} / ${formatBytes(event.totalBytes)}`,
        ),
      );
    }
  };
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

  printWarnings(summary.warnings);
  console.log(`${theme.muted("Log file:")} ${path.resolve(logFilePath)}`);
  console.log(
    `${theme.muted("Progress log:")} ${path.resolve(summary.progressLogPath)}`,
  );
}

export function createImportProgressReporter(): (
  event: ImportProgressEvent,
) => void {
  const frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
  let frameIndex = 0;
  let spinnerTimer: NodeJS.Timeout | undefined;
  let currentLines: string[] = [];
  let renderedLines = 0;
  let lastRenderedBlock = "";

  const shortPath = (value: string, maxLength = 68): string =>
    truncateMiddle(value, maxLength);

  const renderBlock = (lines: string[]): void => {
    const block = lines.join("\n");

    if (!process.stdout.isTTY) {
      if (block !== lastRenderedBlock) {
        console.log(block);
        lastRenderedBlock = block;
      }
      return;
    }

    const width = process.stdout.columns || 120;

    if (renderedLines > 1) {
      process.stdout.write(`\u001B[${renderedLines - 1}F`);
    } else if (renderedLines === 1) {
      process.stdout.write("\r");
    }

    for (let index = 0; index < lines.length; index += 1) {
      process.stdout.write("\u001B[2K");
      process.stdout.write(lines[index]!.padEnd(width));
      if (index < lines.length - 1) {
        process.stdout.write("\n");
      }
    }

    if (renderedLines > lines.length) {
      for (let index = lines.length; index < renderedLines; index += 1) {
        process.stdout.write("\n\u001B[2K");
      }
      if (renderedLines - lines.length > 0) {
        process.stdout.write(`\u001B[${renderedLines - lines.length}F`);
      }
    }

    renderedLines = lines.length;
    lastRenderedBlock = block;
  };

  const stopSpinner = (): void => {
    if (spinnerTimer) {
      clearInterval(spinnerTimer);
      spinnerTimer = undefined;
    }
  };

  const finalizeDynamicOutput = (): void => {
    stopSpinner();
    if (process.stdout.isTTY && renderedLines > 0) {
      process.stdout.write("\n");
    }
    currentLines = [];
    renderedLines = 0;
    lastRenderedBlock = "";
  };

  const startSpinner = (): void => {
    if (spinnerTimer) {
      return;
    }

    spinnerTimer = setInterval(() => {
      if (currentLines.length === 0) {
        return;
      }

      frameIndex += 1;
      const spinner = frames[frameIndex % frames.length] ?? "⠋";
      const nextLines = [...currentLines];
      nextLines[0] = nextLines[0]!.replace("__SPINNER__", theme.blue(spinner));
      renderBlock(nextLines);
    }, 220);
  };

  return (event: ImportProgressEvent): void => {
    if (event.kind === "preparing_start") {
      stopSpinner();
      frameIndex = 0;
      currentLines = [
        `${theme.infoLabel("PREPARING")} __SPINNER__ import plan`,
        `Input: ${shortPath(event.validatedPath)}`,
        `Target: ${event.targetDatabase}`,
        `Datasets: ${formatCount(event.totalDatasets)} | Files: ${formatCount(event.totalFiles)} | Batch size: ${formatCount(event.batchSize)}`,
        `Scanning: 0/${formatCount(event.totalFiles)} files`,
        `Rows counted: ${formatCount(0)}`,
        `Current: waiting...`,
      ];
      renderBlock([
        currentLines[0]!.replace("__SPINNER__", theme.blue(frames[0]!)),
        ...currentLines.slice(1),
      ]);
      startSpinner();
      return;
    }

    if (event.kind === "preparing_progress") {
      currentLines = [
        `${theme.infoLabel("PREPARING")} __SPINNER__ import plan`,
        currentLines[1] ?? "",
        currentLines[2] ?? "",
        currentLines[3] ?? "",
        `Scanning: ${formatCount(event.scannedFiles)}/${formatCount(event.totalFiles)} files`,
        `Rows counted: ${formatCount(event.countedRows)}`,
        `Current: ${shortPath(event.currentFileDisplayPath)}`,
      ];
      renderBlock([
        currentLines[0]!.replace(
          "__SPINNER__",
          theme.blue(frames[frameIndex % frames.length] ?? "⠋"),
        ),
        ...currentLines.slice(1),
      ]);
      return;
    }

    if (event.kind === "plan_ready") {
      stopSpinner();
      renderBlock([
        `${theme.successLabel("PREPARING")} Import plan ready.`,
        `Target: ${event.targetDatabase}`,
        `Datasets: ${formatCount(event.totalDatasets)} | Files: ${formatCount(event.totalFiles)} | Batch size: ${formatCount(event.batchSize)}`,
        `Rows counted exactly: ${formatCount(event.totalRows)}`,
        `Batches planned exactly: ${formatCount(event.totalBatches)}`,
        `Order: ${event.executionOrder.join(" > ")}`,
      ]);
      finalizeDynamicOutput();
      return;
    }

    if (event.kind === "start") {
      console.log(theme.infoLabel("IMPORT"), "Starting database import...");
      console.log(formatKeyValue("Input path", event.inputPath));
      console.log(formatKeyValue("Validated path", event.validatedPath));
      console.log(formatKeyValue("Target database", event.targetDatabase));
      console.log(
        formatKeyValue(
          "Rows committed from checkpoints",
          formatCount(event.committedRows),
        ),
      );
      console.log(
        formatKeyValue(
          "Batches committed from checkpoints",
          `${formatCount(event.committedBatches)} / ${formatCount(event.totalBatches)}`,
        ),
      );
      frameIndex = 0;
      currentLines = [];
      renderedLines = 0;
      lastRenderedBlock = "";
      return;
    }

    if (event.kind === "progress") {
      const spinner = frames[frameIndex % frames.length] ?? "⠋";

      if (event.verboseProgress) {
        currentLines = [
          `${theme.infoLabel("IMPORT")} __SPINNER__ status`,
          `Dataset: ${event.dataset} (${formatCount(event.datasetIndex)}/${formatCount(event.totalDatasets)})`,
          `File: ${formatCount(event.fileIndex)}/${formatCount(event.totalFiles)} | ${shortPath(event.currentFileDisplayPath)}`,
          `Rows: ${formatCount(event.committedRows)} committed | ${formatCount(event.currentFileRowsCommitted)}/${formatCount(event.currentFileRowsTotal)} in file`,
          `Batches: ${formatCount(event.committedBatches)}/${formatCount(event.totalBatches)} | size ${formatCount(event.batchSize)}`,
          `File progress: ${formatBytes(event.checkpointOffset)} / ${formatBytes(event.currentFileSize)} | Checkpoint: saved`,
        ];
      } else {
        currentLines = [
          `${theme.infoLabel("IMPORT")} __SPINNER__ ${event.dataset} | dataset ${formatCount(event.datasetIndex)}/${formatCount(event.totalDatasets)} | file ${formatCount(event.fileIndex)}/${formatCount(event.totalFiles)} | rows ${formatCount(event.committedRows)} | batches ${formatCount(event.committedBatches)}/${formatCount(event.totalBatches)} | current ${shortPath(event.currentFileDisplayPath, 44)}`,
        ];
      }

      renderBlock([
        currentLines[0]!.replace("__SPINNER__", theme.blue(spinner)),
        ...currentLines.slice(1),
      ]);
      startSpinner();
      return;
    }

    finalizeDynamicOutput();
    console.log(
      theme.successLabel("IMPORT"),
      `Processed ${formatCount(event.completedFiles)}/${formatCount(event.totalFiles)} files and ${formatCount(event.processedRows)} row(s).`,
    );
    console.log(
      formatKeyValue(
        "Batches committed",
        `${formatCount(event.committedBatches)} / ${formatCount(event.totalBatches)}`,
      ),
    );
    console.log(
      formatKeyValue(
        "Secondary CNAE rows",
        formatCount(event.secondaryCnaesRows),
      ),
    );
    console.log(
      formatKeyValue("Quarantined rows", formatCount(event.quarantinedRows)),
    );
  };
}

export function handleCliError(error: unknown): never {
  if (error instanceof AppError) {
    console.error(`${theme.errorLabel(error.code)} ${error.message}`);
    process.exit(1);
  }

  const message = error instanceof Error ? error.message : String(error);
  console.error(`${theme.errorLabel("UNEXPECTED_ERROR")} ${message}`);
  process.exit(1);
}
