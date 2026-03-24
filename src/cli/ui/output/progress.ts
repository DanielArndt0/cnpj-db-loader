import { theme } from "../theme.js";
import type { ExtractionProgressEvent } from "../../../services/extract.service.js";
import type { ImportProgressEvent } from "../../../services/import.service.js";
import {
  formatBytes,
  formatCount,
  formatKeyValue,
  truncateMiddle,
} from "./shared.js";

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
        `${theme.successLabel("PREPARING")} ${event.reused ? "Saved import plan reused." : "Import plan ready."}`,
        `Target: ${event.targetDatabase}${event.planId === null ? "" : ` | Plan #${formatCount(event.planId)}`}`,
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
