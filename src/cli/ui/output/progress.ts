import { theme } from "../theme.js";
import type { ExtractionProgressEvent } from "../../../services/extract.service.js";
import type { FederalRevenueDownloadProgressEvent } from "../../../services/federal-revenue/index.js";
import type { ImportProgressEvent } from "../../../services/import.service.js";
import type { SanitizeProgressEvent } from "../../../services/sanitize.service.js";
import {
  formatBytes,
  formatCount,
  formatDuration,
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
      `| ${completedArchives}/${totalArchives} arquivos ` +
      `| restantes ${remainingArchives} ` +
      `| ${bytesProgress} ` +
      `| atual ${archiveLabel}`;

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
      console.log(
        theme.infoLabel("EXTRACT"),
        "Iniciando a extração dos arquivos...",
      );
      console.log(formatKeyValue("Caminho de entrada", event.inputPath));
      console.log(formatKeyValue("Caminho de saída", event.outputPath));
      console.log(formatKeyValue("Arquivos na fila", event.totalArchives));
      console.log(
        formatKeyValue("Bytes dos arquivos", formatBytes(event.totalBytes)),
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
        `${theme.warningLabel("AVISO")} Falha ao extrair ${event.currentArchiveName}: ${event.errorMessage}`,
      );
      return;
    }

    if (event.kind === "finish") {
      finalizeDynamicLine();
      console.log(
        theme.successLabel("EXTRACT"),
        `Processados ${event.completedArchives}/${event.totalArchives} arquivos (${event.failedArchives} com falha).`,
      );
      console.log(formatKeyValue("Caminho de saída", event.outputPath));
      console.log(
        formatKeyValue(
          "Bytes dos arquivos",
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

  const formatPlanBatchLine = (event: {
    totalDatasets: number;
    totalFiles: number;
    batchSize: number;
    loadBatchSize?: number;
    materializeBatchSize?: number;
  }): string => {
    if (
      typeof event.loadBatchSize === "number" &&
      typeof event.materializeBatchSize === "number"
    ) {
      return `Conjuntos: ${formatCount(event.totalDatasets)} | Arquivos: ${formatCount(event.totalFiles)} | Lote de carga: ${formatCount(event.loadBatchSize)} | Lote de materialização: ${formatCount(event.materializeBatchSize)}`;
    }

    if (typeof event.loadBatchSize === "number") {
      return `Conjuntos: ${formatCount(event.totalDatasets)} | Arquivos: ${formatCount(event.totalFiles)} | Tamanho do lote: ${formatCount(event.loadBatchSize)}`;
    }

    return `Conjuntos: ${formatCount(event.totalDatasets)} | Arquivos: ${formatCount(event.totalFiles)} | Tamanho do lote: ${formatCount(event.batchSize)}`;
  };

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
        `${theme.infoLabel("PREPARANDO")} __SPINNER__ plano de importação`,
        `Entrada: ${shortPath(event.validatedPath)}`,
        `Destino: ${event.targetDatabase}`,
        formatPlanBatchLine(event),
        `Varrendo: 0/${formatCount(event.totalFiles)} arquivos`,
        `Linhas contadas: ${formatCount(0)}`,
        `Atual: aguardando...`,
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
        `${theme.infoLabel("PREPARANDO")} __SPINNER__ plano de importação`,
        currentLines[1] ?? "",
        currentLines[2] ?? "",
        currentLines[3] ?? "",
        `Varrendo: ${formatCount(event.scannedFiles)}/${formatCount(event.totalFiles)} arquivos`,
        `Linhas contadas: ${formatCount(event.countedRows)}`,
        `Atual: ${shortPath(event.currentFileDisplayPath)}`,
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
        `${theme.successLabel("PREPARANDO")} ${event.reused ? "Plano de importação salvo reutilizado." : "Plano de importação pronto."}`,
        `Destino: ${event.targetDatabase}${event.planId === null ? "" : ` | Plano #${formatCount(event.planId)}`}`,
        formatPlanBatchLine(event),
        `Linhas contadas exatamente: ${formatCount(event.totalRows)}`,
        `Lotes planejados exatamente: ${formatCount(event.totalBatches)}`,
        `Ordem: ${event.executionOrder.join(" > ")}`,
      ]);
      finalizeDynamicOutput();
      return;
    }

    if (event.kind === "start") {
      console.log(
        theme.infoLabel("IMPORT"),
        "Iniciando a importação para o banco...",
      );
      console.log(formatKeyValue("Caminho de entrada", event.inputPath));
      console.log(formatKeyValue("Caminho validado", event.validatedPath));
      console.log(formatKeyValue("Banco de destino", event.targetDatabase));
      console.log(
        formatKeyValue(
          "Linhas confirmadas dos checkpoints",
          formatCount(event.committedRows),
        ),
      );
      console.log(
        formatKeyValue(
          "Lotes confirmados dos checkpoints",
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
          `Conjunto: ${event.dataset} (${formatCount(event.datasetIndex)}/${formatCount(event.totalDatasets)})`,
          `Arquivo: ${formatCount(event.fileIndex)}/${formatCount(event.totalFiles)} | ${shortPath(event.currentFileDisplayPath)}`,
          `Linhas: ${formatCount(event.committedRows)} confirmadas | ${formatCount(event.currentFileRowsCommitted)}/${formatCount(event.currentFileRowsTotal)} no arquivo`,
          `Lotes: ${formatCount(event.committedBatches)}/${formatCount(event.totalBatches)} | tamanho ${formatCount(event.batchSize)}`,
          `Progresso do arquivo: ${formatBytes(event.checkpointOffset)} / ${formatBytes(event.currentFileSize)} | Checkpoint: salvo`,
        ];
      } else {
        currentLines = [
          `${theme.infoLabel("IMPORT")} __SPINNER__ ${event.dataset} | conjunto ${formatCount(event.datasetIndex)}/${formatCount(event.totalDatasets)} | arquivo ${formatCount(event.fileIndex)}/${formatCount(event.totalFiles)} | linhas ${formatCount(event.committedRows)} | lotes ${formatCount(event.committedBatches)}/${formatCount(event.totalBatches)} | atual ${shortPath(event.currentFileDisplayPath, 44)}`,
        ];
      }

      renderBlock([
        currentLines[0]!.replace("__SPINNER__", theme.blue(spinner)),
        ...currentLines.slice(1),
      ]);
      startSpinner();
      return;
    }

    if (event.kind === "materialization_start") {
      currentLines = [
        `${theme.infoLabel("MATERIALIZANDO")} __SPINNER__ staging -> final`,
        `Conjuntos: ${event.datasets.join(" > ")}`,
        `Arquivos importados: ${formatCount(event.completedFiles)}/${formatCount(event.totalFiles)} | Linhas: ${formatCount(event.processedRows)}/${formatCount(event.totalRows)}`,
        `Lotes confirmados: ${formatCount(event.committedBatches)}/${formatCount(event.totalBatches)}`,
        `Atual: aguardando a materialização final...`,
      ];
      renderBlock([
        currentLines[0]!.replace(
          "__SPINNER__",
          theme.blue(frames[frameIndex % frames.length] ?? "⠋"),
        ),
        ...currentLines.slice(1),
      ]);
      startSpinner();
      return;
    }

    if (event.kind === "materialization_progress") {
      const rowsLine =
        typeof event.rowsMaterialized === "number"
          ? `Linhas materializadas: ${formatCount(event.rowsMaterialized)}${typeof event.datasetRowCount === "number" ? ` / ${formatCount(event.datasetRowCount)}` : ""}`
          : `Linhas materializadas: aguardando...`;
      const chunksLine =
        typeof event.chunksCompleted === "number"
          ? `Blocos: ${formatCount(event.chunksCompleted)}${typeof event.estimatedChunks === "number" ? ` / ${formatCount(event.estimatedChunks)}` : ""}${typeof event.chunkSize === "number" ? ` | tamanho ${formatCount(event.chunkSize)}` : ""}`
          : `Blocos: aguardando...${typeof event.chunkSize === "number" ? ` | tamanho ${formatCount(event.chunkSize)}` : ""}`;
      const cursorLine =
        typeof event.lastStagingId === "number"
          ? `Último staging id: ${formatCount(event.lastStagingId)}`
          : `Último staging id: aguardando...`;

      currentLines = [
        `${theme.infoLabel("MATERIALIZANDO")} __SPINNER__ status`,
        `Conjunto: ${event.dataset} (${formatCount(event.datasetIndex)}/${formatCount(event.totalDatasets)}) | concluídos ${formatCount(event.completedDatasets)}/${formatCount(event.totalDatasets)}`,
        `Tabela de destino: ${event.targetTable}`,
        rowsLine,
        chunksLine,
        cursorLine,
        `Etapa: ${event.stepLabel}${event.elapsedMs === undefined ? "" : ` | decorrido ${formatDuration(event.elapsedMs)}`}`,
        `Motivo: ${event.reason ?? "Executando a próxima etapa de materialização deste conjunto."}`,
      ];
      renderBlock([
        currentLines[0]!.replace(
          "__SPINNER__",
          theme.blue(frames[frameIndex % frames.length] ?? "⠋"),
        ),
        ...currentLines.slice(1),
      ]);
      startSpinner();
      return;
    }

    if (event.kind === "materialization_finish") {
      finalizeDynamicOutput();
      console.log(
        theme.successLabel("MATERIALIZANDO"),
        `Concluídos ${formatCount(event.completedDatasets)}/${formatCount(event.totalDatasets)} conjunto(s) de staging.`,
      );
      return;
    }

    finalizeDynamicOutput();
    console.log(
      theme.successLabel("IMPORT"),
      `Processados ${formatCount(event.completedFiles)}/${formatCount(event.totalFiles)} arquivos e ${formatCount(event.processedRows)} linha(s).`,
    );
    console.log(
      formatKeyValue(
        "Lotes confirmados",
        `${formatCount(event.committedBatches)} / ${formatCount(event.totalBatches)}`,
      ),
    );
    console.log(
      formatKeyValue(
        "Linhas em quarentena",
        formatCount(event.quarantinedRows),
      ),
    );
  };
}

export function createSanitizeProgressReporter(): (
  event: SanitizeProgressEvent,
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

  return (event: SanitizeProgressEvent): void => {
    if (event.kind === "start") {
      frameIndex = 0;
      currentLines = [
        `${theme.infoLabel("SANITIZE")} __SPINNER__ preparando dataset sanitizado`,
        `Validado: ${shortPath(event.validatedPath)}`,
        `Saída: ${shortPath(event.outputPath)}`,
        `Conjuntos: ${event.datasets.join(" > ")}`,
        `Encoding de origem: ${event.sourceEncoding} > UTF8`,
        `Arquivos: 0/${formatCount(event.totalFiles)} | Bytes: ${formatBytes(0)} / ${formatBytes(event.totalBytes)}`,
        `Linhas: ${formatCount(0)} | NUL: ${formatCount(0)} | Bytes inválidos: ${formatCount(0)} | Controles: ${formatCount(0)} | Caracteres de substituição: ${formatCount(0)}`,
        `Atual: aguardando...`,
      ];
      renderBlock([
        currentLines[0]!.replace("__SPINNER__", theme.blue(frames[0]!)),
        ...currentLines.slice(1),
      ]);
      startSpinner();
      return;
    }

    if (event.kind === "progress") {
      currentLines = [
        `${theme.infoLabel("SANITIZE")} __SPINNER__ status`,
        currentLines[1] ?? "",
        currentLines[2] ?? "",
        currentLines[3] ?? "",
        currentLines[4] ?? "",
        `Arquivos: ${formatCount(event.fileIndex)}/${formatCount(event.totalFiles)} | Bytes: ${formatBytes(event.bytesProcessed)} / ${formatBytes(event.totalBytes)}`,
        `Linhas: ${formatCount(event.processedRows)} | NUL: ${formatCount(event.nulBytesRemoved)} | Bytes inválidos: ${formatCount(event.invalidBytesRemoved)} | Controles: ${formatCount(event.controlCharsRemoved)} | Caracteres de substituição: ${formatCount(event.replacementCharactersFound)} | Alterados: ${formatCount(event.changedFiles)}`,
        `Atual: ${shortPath(event.currentFileDisplayPath)}`,
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

    finalizeDynamicOutput();
    console.log(
      theme.successLabel("SANITIZE"),
      `Preparados ${formatCount(event.totalFiles)} arquivo(s) e contadas ${formatCount(event.processedRows)} linha(s).`,
    );
    console.log(
      formatKeyValue("Bytes NUL removidos", formatCount(event.nulBytesRemoved)),
    );
    console.log(
      formatKeyValue(
        "Bytes inválidos removidos",
        formatCount(event.invalidBytesRemoved),
      ),
    );
    console.log(
      formatKeyValue(
        "Caracteres de controle removidos",
        formatCount(event.controlCharsRemoved),
      ),
    );
    console.log(
      formatKeyValue(
        "Caracteres de substituição encontrados",
        formatCount(event.replacementCharactersFound),
      ),
    );
    console.log(
      formatKeyValue(
        "Caracteres de substituição restantes",
        formatCount(event.replacementCharactersRemaining),
      ),
    );
    console.log(
      formatKeyValue("Arquivos alterados", formatCount(event.changedFiles)),
    );
    console.log(
      formatKeyValue("Bytes processados", formatBytes(event.totalBytes)),
    );
  };
}

export function createFederalRevenueDownloadProgressReporter(): (
  event: FederalRevenueDownloadProgressEvent,
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

  return (event: FederalRevenueDownloadProgressEvent): void => {
    if (event.kind === "start") {
      frameIndex = 0;
      currentLines = [
        `${theme.infoLabel("RECEITA FEDERAL")} __SPINNER__ baixando ${event.reference}`,
        `Saída: ${shortPath(event.outputPath)}`,
        `Arquivos: 0/${formatCount(event.totalFiles)}`,
        `Bytes: ${formatBytes(0)} / ${formatBytes(event.totalBytes)}`,
        `Atual: aguardando...`,
      ];
      renderBlock([
        currentLines[0]!.replace("__SPINNER__", theme.blue(frames[0]!)),
        ...currentLines.slice(1),
      ]);
      startSpinner();
      return;
    }

    if (event.kind === "file-start") {
      currentLines = [
        `${theme.infoLabel("RECEITA FEDERAL")} __SPINNER__ baixando ${event.reference}`,
        currentLines[1] ?? "",
        `Arquivos: ${formatCount(event.completedFiles)}/${formatCount(event.totalFiles)}`,
        `Bytes: ${formatBytes(event.downloadedBytes)} / ${formatBytes(event.totalBytes)}`,
        `Atual: ${shortPath(event.fileName)}`,
      ];
      renderBlock([
        currentLines[0]!.replace(
          "__SPINNER__",
          theme.blue(frames[frameIndex % frames.length] ?? "⠋"),
        ),
        ...currentLines.slice(1),
      ]);
      startSpinner();
      return;
    }

    if (event.kind === "file-failed") {
      finalizeDynamicOutput();
      console.log(
        `${theme.warningLabel("AVISO")} Falha ao baixar ${event.fileName}: ${event.errorMessage}`,
      );
      return;
    }

    if (event.kind === "file-complete" || event.kind === "file-skipped") {
      currentLines = [
        `${theme.infoLabel("RECEITA FEDERAL")} __SPINNER__ baixando ${event.reference}`,
        currentLines[1] ?? "",
        `Arquivos: ${formatCount(event.completedFiles)}/${formatCount(event.totalFiles)}`,
        `Bytes: ${formatBytes(event.downloadedBytes)} / ${formatBytes(event.totalBytes)}`,
        `Atual: ${event.kind === "file-skipped" ? "ignorado" : "baixado"} ${shortPath(event.fileName)}`,
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

    finalizeDynamicOutput();
    console.log(
      theme.successLabel("RECEITA FEDERAL"),
      `Processados ${formatCount(event.totalFiles)} arquivo(s) para ${event.reference}.`,
    );
    console.log(formatKeyValue("Arquivos baixados", event.downloadedFiles));
    console.log(formatKeyValue("Arquivos ignorados", event.skippedFiles));
    console.log(formatKeyValue("Arquivos falhos", event.failedFiles));
    console.log(
      formatKeyValue(
        "Bytes processados",
        `${formatBytes(event.downloadedBytes)} / ${formatBytes(event.totalBytes)}`,
      ),
    );
  };
}

import type {
  PostgresCsvExportProgressEvent,
  PostgresDirectScriptProgressEvent,
} from "../../../services/postgres-direct/index.js";

export function createPostgresCsvExportProgressReporter(): (
  event: PostgresCsvExportProgressEvent,
) => void {
  return (event: PostgresCsvExportProgressEvent): void => {
    if (event.kind === "start") {
      console.log(
        theme.infoLabel("POSTGRES"),
        "Iniciando a exportação de CSV pronto para o PostgreSQL...",
      );
      console.log(formatKeyValue("Caminho de entrada", event.inputPath));
      console.log(formatKeyValue("Caminho validado", event.validatedPath));
      console.log(formatKeyValue("Caminho de saída", event.outputPath));
      console.log(formatKeyValue("Arquivos na fila", event.totalFiles));
      return;
    }

    if (event.kind === "file_finish") {
      console.log(
        `${theme.infoLabel("POSTGRES")} ${event.fileIndex}/${event.totalFiles} ${event.dataset} exportado com ${formatCount(event.rows)} linha(s).`,
      );
      return;
    }

    if (event.kind === "finish") {
      console.log(
        theme.successLabel("POSTGRES"),
        `Exportados ${event.totalFiles} arquivo(s) com ${formatCount(event.totalRows)} linha(s).`,
      );
      console.log(formatKeyValue("Caminho de saída", event.outputPath));
      console.log(formatKeyValue("Caminho do script", event.scriptPath));
    }
  };
}

export function createPostgresDirectScriptProgressReporter(): (
  event: PostgresDirectScriptProgressEvent,
) => void {
  return (event: PostgresDirectScriptProgressEvent): void => {
    if (event.kind === "start") {
      console.log(
        theme.infoLabel("POSTGRES"),
        "Iniciando a geração do script direto do PostgreSQL...",
      );
      console.log(formatKeyValue("Caminho de entrada", event.inputPath));
      console.log(formatKeyValue("Caminho validado", event.validatedPath));
      console.log(formatKeyValue("Caminho de saída", event.outputPath));
      console.log(formatKeyValue("Encoding de origem", event.sourceEncoding));
      console.log(formatKeyValue("Modo de transação", event.transactionMode));
      console.log(formatKeyValue("Etapas incluídas", event.include.join(", ")));
      console.log(
        formatKeyValue("Pular índices", event.skipIndexes ? "sim" : "não"),
      );
      console.log(
        formatKeyValue("Pular analyze", event.skipAnalyze ? "sim" : "não"),
      );
      console.log(formatKeyValue("Arquivos na fila", event.totalFiles));
      return;
    }

    if (event.kind === "file_registered") {
      console.log(
        `${theme.infoLabel("POSTGRES")} ${event.fileIndex}/${event.totalFiles} ${event.dataset} registrado (${formatBytes(event.fileSize)}).`,
      );
      return;
    }

    if (event.kind === "finish") {
      console.log(
        theme.successLabel("POSTGRES"),
        `Script de importação direta gerado para ${event.totalFiles} arquivo(s) (${formatBytes(event.totalBytes)}).`,
      );
      console.log(formatKeyValue("Caminho de saída", event.outputPath));
      console.log(formatKeyValue("Caminho do script", event.scriptPath));
    }
  };
}
