import { theme } from "../theme.js";
import type { ExtractionSummary } from "../../../services/extract.service.js";
import type {
  FederalRevenueCheckSummary,
  FederalRevenueCleanSummary,
  FederalRevenueDownloadSummary,
  FederalRevenueStatusSummary,
  FederalRevenueSyncSummary,
} from "../../../services/federal-revenue/index.js";
import type { InspectSummary } from "../../../services/inspect.service.js";
import type { ImportSummary } from "../../../services/import.service.js";
import type {
  PostgresCsvExportSummary,
  PostgresDirectScriptSummary,
} from "../../../services/postgres-direct/index.js";
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
  printNotes,
  resolveLogFilePath,
} from "./shared.js";

export function printInspectSummary(
  summary: InspectSummary,
  logFilePath: string,
): void {
  console.log(theme.successLabel("INSPECT"), "Inspeção concluída.");
  console.log(formatKeyValue("Caminho de entrada", summary.inputPath));
  console.log(formatKeyValue("Modo detectado", summary.detectedInputMode));
  console.log(formatKeyValue("Total de entradas", summary.totalEntries));
  console.log(formatKeyValue("Arquivos ZIP", summary.zipArchivesFound));
  console.log(
    formatKeyValue(
      "Entradas extraídas reconhecidas",
      summary.extractedEntriesFound,
    ),
  );

  const recognizedDatasets = Object.entries(summary.recognizedDatasets);
  if (recognizedDatasets.length > 0) {
    console.log(theme.infoLabel("CONJUNTOS"));
    for (const [dataset, count] of recognizedDatasets) {
      console.log(`  ${theme.blue("•")} ${dataset}: ${count}`);
    }
  }

  printWarnings(summary.warnings);
  if (summary.nextStep) {
    console.log(`${theme.infoLabel("PRÓXIMO")} ${summary.nextStep}`);
  }

  console.log(
    `${theme.muted("Arquivo de log:")} ${resolveLogFilePath(logFilePath)}`,
  );
}

export function printExtractionSummary(
  summary: ExtractionSummary,
  logFilePath: string,
): void {
  console.log(theme.successLabel("EXTRACT"), "Extração concluída.");
  console.log(formatKeyValue("Caminho de entrada", summary.inputPath));
  console.log(formatKeyValue("Caminho de saída", summary.outputPath));
  console.log(formatKeyValue("Sistema operacional", summary.operatingSystem));
  console.log(
    formatKeyValue("Arquivos ZIP encontrados", summary.zipFilesFound),
  );
  console.log(
    formatKeyValue("Arquivos extraídos", summary.extractedArchives.length),
  );
  console.log(
    formatKeyValue("Arquivos com falha", summary.failedArchives.length),
  );
  console.log(
    formatKeyValue(
      "Bytes de arquivos processados",
      `${formatBytes(summary.extractedArchiveBytes)} / ${formatBytes(summary.totalArchiveBytes)}`,
    ),
  );

  printWarnings(
    summary.failedArchives.length > 0
      ? [
          "Alguns arquivos não puderam ser extraídos. Verifique o arquivo de log para detalhes.",
        ]
      : [],
  );

  console.log(
    `${theme.muted("Arquivo de log:")} ${resolveLogFilePath(logFilePath)}`,
  );
}

export function printValidationSummary(
  summary: ValidationSummary,
  logFilePath: string,
): void {
  console.log(
    theme.successLabel("VALIDATE"),
    summary.ok ? "Validação concluída." : "Validação concluída com erros.",
  );
  console.log(
    formatKeyValue("Caminho de entrada", summary.inspected.inputPath),
  );
  console.log(formatKeyValue("Caminho validado", summary.validatedPath));
  console.log(
    formatKeyValue("Modo detectado", summary.inspected.detectedInputMode),
  );
  console.log(
    formatKeyValue("Total de entradas", summary.inspected.totalEntries),
  );
  console.log(
    formatKeyValue("Conjuntos reconhecidos", summary.presentDatasets.length),
  );
  console.log(
    formatKeyValue("Conjuntos ausentes", summary.missingDatasets.length),
  );
  console.log(formatKeyValue("Errors", summary.errors.length));
  console.log(formatKeyValue("Warnings", summary.warnings.length));

  if (summary.presentDatasets.length > 0) {
    console.log(theme.infoLabel("CONJUNTOS"));
    for (const dataset of summary.presentDatasets) {
      console.log(`  ${theme.blue("•")} ${dataset}`);
    }
  }

  if (summary.missingDatasets.length > 0) {
    console.log(theme.warningLabel("AUSENTES"));
    for (const dataset of summary.missingDatasets) {
      console.log(`  ${theme.yellow("•")} ${dataset}`);
    }
  }

  printErrors(summary.errors);
  printWarnings(summary.warnings);
  if (summary.nextStep) {
    console.log(`${theme.infoLabel("PRÓXIMO")} ${summary.nextStep}`);
  }
  console.log(
    `${theme.muted("Arquivo de log:")} ${resolveLogFilePath(logFilePath)}`,
  );
}

export function printDatabaseConfigSummary(
  config: { defaultDbUrl?: string },
  logFilePath: string,
): void {
  console.log(
    theme.successLabel("DATABASE"),
    "Configuração de banco carregada.",
  );
  console.log(
    formatKeyValue(
      "URL padrão do banco",
      config.defaultDbUrl ?? "não configurado",
    ),
  );
  console.log(
    `${theme.muted("Arquivo de log:")} ${resolveLogFilePath(logFilePath)}`,
  );
}

export function printFederalRevenueConfigSummary(
  config: {
    webdavUrl: string;
    userAgent: string;
    shareToken?: string | undefined;
    configured: {
      webdavUrl: boolean;
      userAgent: boolean;
      shareToken: boolean;
    };
  },
  logFilePath: string,
): void {
  console.log(
    theme.successLabel("RECEITA FEDERAL"),
    "Configuração da Receita Federal carregada.",
  );
  console.log(
    formatKeyValue(
      "WebDAV URL",
      `${config.webdavUrl}${config.configured.webdavUrl ? "" : " (padrão)"}`,
    ),
  );
  console.log(
    formatKeyValue(
      "User agent",
      `${config.userAgent}${config.configured.userAgent ? "" : " (padrão)"}`,
    ),
  );
  console.log(
    formatKeyValue(
      "Token de compartilhamento",
      config.shareToken ?? "não configurado",
    ),
  );
  console.log(
    `${theme.muted("Arquivo de log:")} ${resolveLogFilePath(logFilePath)}`,
  );
}

export function printDatabaseCleanupSummary(
  summary: {
    scope: string;
    targetDatabase: string;
    dataset?: string | undefined;
    phase?: string | undefined;
    validatedPath?: string | undefined;
    planId?: number | undefined;
    truncatedTables: string[];
    deletedLoadCheckpoints: number;
    deletedMaterializationCheckpoints: number;
    deletedPlans: number;
    notes: string[];
  },
  logFilePath: string,
): void {
  console.log(
    theme.successLabel("DATABASE"),
    `Limpeza concluída para ${summary.scope}.`,
  );
  console.log(formatKeyValue("Banco de destino", summary.targetDatabase));
  console.log(formatKeyValue("Escopo", summary.scope));

  if (summary.dataset) {
    console.log(formatKeyValue("Dataset", summary.dataset));
  }

  if (summary.phase) {
    console.log(formatKeyValue("Fase do checkpoint", summary.phase));
  }

  if (summary.planId !== undefined) {
    console.log(formatKeyValue("Id do plano", summary.planId));
  }

  if (summary.validatedPath) {
    console.log(formatKeyValue("Caminho validado", summary.validatedPath));
  }

  console.log(
    formatKeyValue(
      "Tabelas de staging/finais truncadas",
      summary.truncatedTables.length,
    ),
  );
  console.log(
    formatKeyValue(
      "Checkpoints de carga excluídos",
      formatCount(summary.deletedLoadCheckpoints),
    ),
  );
  console.log(
    formatKeyValue(
      "Checkpoints de materialização excluídos",
      formatCount(summary.deletedMaterializationCheckpoints),
    ),
  );
  console.log(
    formatKeyValue(
      "Planos de importação excluídos",
      formatCount(summary.deletedPlans),
    ),
  );

  if (summary.truncatedTables.length > 0) {
    console.log(theme.warningLabel("TABELAS"));
    for (const tableName of summary.truncatedTables) {
      console.log(`  ${theme.yellow("•")} ${tableName}`);
    }
  }

  printNotes(summary.notes);
  console.log(
    `${theme.muted("Arquivo de log:")} ${resolveLogFilePath(logFilePath)}`,
  );
}

export function printInfoWithLog(
  label: string,
  message: string,
  logFilePath: string,
): void {
  console.log(theme.successLabel(label), message);
  console.log(
    `${theme.muted("Arquivo de log:")} ${resolveLogFilePath(logFilePath)}`,
  );
}

export function printSanitizeSummary(
  summary: SanitizeSummary,
  logFilePath: string,
): void {
  console.log(
    theme.successLabel("SANITIZE"),
    "Sanitização do dataset concluída.",
  );
  console.log(formatKeyValue("Caminho de entrada", summary.inputPath));
  console.log(formatKeyValue("Caminho validado", summary.validatedPath));
  console.log(formatKeyValue("Caminho de saída", summary.outputPath));
  console.log(formatKeyValue("Arquivos processados", summary.processedFiles));
  console.log(
    formatKeyValue("Linhas contadas", formatCount(summary.processedRows)),
  );
  console.log(
    formatKeyValue("Bytes processados", formatBytes(summary.totalBytes)),
  );
  console.log(formatKeyValue("Encoding de origem", summary.sourceEncoding));
  console.log(formatKeyValue("Encoding de saída", "UTF8"));
  console.log(
    formatKeyValue("Bytes NUL removidos", formatCount(summary.nulBytesRemoved)),
  );
  console.log(
    formatKeyValue(
      "Bytes inválidos removidos",
      formatCount(summary.invalidBytesRemoved),
    ),
  );
  console.log(
    formatKeyValue(
      "Caracteres de controle removidos",
      formatCount(summary.controlCharsRemoved),
    ),
  );
  console.log(
    formatKeyValue(
      "Caracteres de substituição encontrados",
      formatCount(summary.replacementCharactersFound),
    ),
  );
  console.log(
    formatKeyValue(
      "Caracteres de substituição restantes",
      formatCount(summary.replacementCharactersRemaining),
    ),
  );
  console.log(formatKeyValue("Arquivos alterados", summary.changedFiles));
  console.log(formatKeyValue("Arquivos inalterados", summary.unchangedFiles));

  if (summary.datasets.length > 0) {
    console.log(theme.infoLabel("CONJUNTOS"));
    for (const dataset of summary.datasets) {
      console.log(`  ${theme.blue("•")} ${dataset}`);
    }
  }

  printWarnings(summary.warnings);
  if (summary.nextStep) {
    console.log(`${theme.infoLabel("PRÓXIMO")} ${summary.nextStep}`);
  }

  console.log(
    `${theme.muted("Arquivo de log:")} ${resolveLogFilePath(logFilePath)}`,
  );
}

export function printImportSummary(
  summary: ImportSummary,
  logFilePath: string,
): void {
  const headline =
    summary.executionMode === "load"
      ? "Carga de staging/direta concluída."
      : summary.executionMode === "materialize"
        ? "Materialização do staging concluída."
        : "Importação para o banco concluída.";

  console.log(theme.successLabel("IMPORT"), headline);
  console.log(formatKeyValue("Caminho de entrada", summary.inputPath));
  console.log(formatKeyValue("Caminho validado", summary.validatedPath));
  console.log(formatKeyValue("Banco de destino", summary.targetDatabase));
  console.log(
    formatKeyValue("Conjuntos importados", summary.importedDatasets.length),
  );
  console.log(formatKeyValue("Arquivos importados", summary.importedFiles));
  console.log(
    formatKeyValue("Linhas confirmadas", formatCount(summary.processedRows)),
  );
  console.log(
    formatKeyValue("Linhas planejadas", formatCount(summary.plannedRows)),
  );
  console.log(
    formatKeyValue(
      "Lotes confirmados",
      `${formatCount(summary.committedBatches)} / ${formatCount(summary.plannedBatches)}`,
    ),
  );
  console.log(
    formatKeyValue(
      "Linhas em quarentena",
      formatCount(summary.quarantinedRows),
    ),
  );
  console.log(formatKeyValue("Arquivos retomados", summary.resumedFiles));
  console.log(
    formatKeyValue(
      "Arquivos com checkpoint completo",
      summary.skippedCompletedFiles,
    ),
  );

  if (summary.datasetSummaries.length > 0) {
    console.log(theme.infoLabel("CONJUNTOS"));
    for (const datasetSummary of summary.datasetSummaries) {
      console.log(
        `  ${theme.blue("•")} ${datasetSummary.dataset}: ${datasetSummary.files} arquivo(s), ${formatCount(datasetSummary.rows)} linha(s)`,
      );
    }
  }

  console.log(theme.infoLabel("DESEMPENHO"));
  console.log(
    formatKeyValue(
      "Duração total",
      formatDuration(summary.performance.totalDurationMs),
    ),
  );
  console.log(
    formatKeyValue(
      "Varredura preparatória",
      formatDuration(summary.performance.scanDurationMs),
    ),
  );
  console.log(
    formatKeyValue(
      "Execução da importação",
      formatDuration(summary.performance.executionDurationMs),
    ),
  );
  console.log(
    formatKeyValue(
      "Carga de lookups",
      formatDuration(summary.performance.lookupLoadDurationMs),
    ),
  );
  console.log(
    formatKeyValue(
      "Caminho de insert",
      formatDuration(summary.performance.insertDurationMs),
    ),
  );
  console.log(
    formatKeyValue(
      "Caminho de retry",
      formatDuration(summary.performance.retryDurationMs),
    ),
  );
  console.log(
    formatKeyValue(
      "Caminho de quarentena",
      formatDuration(summary.performance.quarantineDurationMs),
    ),
  );
  console.log(
    formatKeyValue(
      "Materialização",
      formatDuration(summary.performance.materializationDurationMs),
    ),
  );
  console.log(
    formatKeyValue(
      "Vazão",
      `${formatRate(summary.performance.rowsPerSecond, "rows/s")} | ${formatRate(summary.performance.batchesPerMinute, "batches/min")}`,
    ),
  );

  if (summary.performance.datasets.length > 0) {
    console.log(theme.infoLabel("DESEMPENHO POR CONJUNTO"));
    for (const datasetPerformance of summary.performance.datasets) {
      console.log(
        `  ${theme.blue("•")} ${datasetPerformance.dataset}: ${formatCount(datasetPerformance.importedRows)} linha(s), ${formatCount(datasetPerformance.committedBatches)} lote(s), ${formatDuration(datasetPerformance.importDurationMs)}, ${formatRate(datasetPerformance.rowsPerSecond, "rows/s")}`,
      );
      console.log(
        `    varredura ${formatDuration(datasetPerformance.scanDurationMs)} | insert ${formatDuration(datasetPerformance.insertDurationMs)} | materialização ${formatDuration(datasetPerformance.materializationDurationMs)} | retry ${formatDuration(datasetPerformance.retryDurationMs)} | quarentena ${formatDuration(datasetPerformance.quarantineDurationMs)} | retomados ${formatCount(datasetPerformance.resumedFiles)} | ignorados ${formatCount(datasetPerformance.skippedCompletedFiles)}`,
      );
    }
  }

  printNotes(summary.warnings);
  console.log(
    `${theme.muted("Arquivo de log:")} ${resolveLogFilePath(logFilePath)}`,
  );
  console.log(
    `${theme.muted("Progress log:")} ${resolveLogFilePath(summary.progressLogPath)}`,
  );
}

export function printFederalRevenueCheckSummary(
  summary: FederalRevenueCheckSummary,
  logFilePath: string,
): void {
  console.log(
    theme.successLabel("RECEITA FEDERAL"),
    "Verificação do dataset remoto concluída.",
  );
  console.log(formatKeyValue("URL base remota", summary.remoteBaseUrl));
  console.log(
    formatKeyValue("Referência selecionada", summary.selectedReference),
  );
  console.log(formatKeyValue("Modo de seleção", summary.selectionMode));
  console.log(
    formatKeyValue(
      "Referências disponíveis",
      summary.availableReferences.length,
    ),
  );
  console.log(formatKeyValue("Arquivos ZIP", summary.totalFiles));
  console.log(formatKeyValue("Bytes remotos", formatBytes(summary.totalBytes)));

  if (summary.files.length > 0) {
    console.log(theme.infoLabel("ARQUIVOS"));
    for (const file of summary.files.slice(0, 20)) {
      const sizeLabel =
        file.sizeInBytes === undefined
          ? "tamanho desconhecido"
          : formatBytes(file.sizeInBytes);
      console.log(`  ${theme.blue("•")} ${file.name} (${sizeLabel})`);
    }

    if (summary.files.length > 20) {
      console.log(
        `  ${theme.muted(`... ${summary.files.length - 20} arquivo(s) adicional(is) omitido(s) da saída do terminal`)}`,
      );
    }
  }

  console.log(
    `${theme.muted("Arquivo de log:")} ${resolveLogFilePath(logFilePath)}`,
  );
}

export function printFederalRevenueDownloadSummary(
  summary: FederalRevenueDownloadSummary,
  logFilePath: string,
): void {
  console.log(
    theme.successLabel("RECEITA FEDERAL"),
    summary.failedFiles > 0
      ? "Download concluído com erros."
      : "Download concluído.",
  );
  console.log(formatKeyValue("Referência", summary.reference));
  console.log(formatKeyValue("Modo de seleção", summary.selectionMode));
  console.log(formatKeyValue("Caminho de saída", summary.outputPath));
  console.log(formatKeyValue("Manifesto", summary.manifestPath));
  console.log(formatKeyValue("ZIP files found", summary.filesFound));
  console.log(formatKeyValue("Arquivos baixados", summary.downloadedFiles));
  console.log(formatKeyValue("Arquivos ignorados", summary.skippedFiles));
  console.log(formatKeyValue("Arquivos falhos", summary.failedFiles));
  console.log(formatKeyValue("Arquivos parciais", summary.partialFiles));
  console.log(formatKeyValue("Arquivos ausentes", summary.missingFiles));
  console.log(
    formatKeyValue(
      "Bytes processados",
      `${formatBytes(summary.downloadedBytes)} / ${formatBytes(summary.totalBytes)}`,
    ),
  );

  printWarnings(summary.warnings);
  if (summary.nextStep) {
    console.log(`${theme.infoLabel("PRÓXIMO")} ${summary.nextStep}`);
  }

  console.log(
    `${theme.muted("Arquivo de log:")} ${resolveLogFilePath(logFilePath)}`,
  );
}

export function printFederalRevenueStatusSummary(
  summary: FederalRevenueStatusSummary,
  logFilePath: string,
): void {
  console.log(
    summary.isComplete
      ? theme.successLabel("RECEITA FEDERAL")
      : theme.warningLabel("RECEITA FEDERAL"),
    summary.isComplete
      ? "A referência local está completa."
      : "A referência local está incompleta.",
  );
  console.log(formatKeyValue("Referência", summary.reference));
  console.log(formatKeyValue("Modo de seleção", summary.selectionMode));
  console.log(formatKeyValue("Caminho de saída", summary.outputPath));
  console.log(formatKeyValue("Manifesto", summary.manifestPath));
  console.log(
    formatKeyValue(
      "Manifesto encontrado",
      summary.manifestFound ? "sim" : "não",
    ),
  );
  console.log(formatKeyValue("Arquivos ZIP", summary.filesFound));
  console.log(formatKeyValue("Arquivos baixados", summary.downloadedFiles));
  console.log(formatKeyValue("Arquivos falhos", summary.failedFiles));
  console.log(formatKeyValue("Arquivos parciais", summary.partialFiles));
  console.log(formatKeyValue("Arquivos ausentes", summary.missingFiles));
  console.log(
    formatKeyValue(
      "Bytes locais",
      `${formatBytes(summary.localBytes)} / ${formatBytes(summary.totalBytes)}`,
    ),
  );

  if (summary.lastCommand) {
    console.log(formatKeyValue("Último comando", summary.lastCommand));
  }

  if (summary.lastStatus) {
    console.log(formatKeyValue("Último status", summary.lastStatus));
  }

  if (summary.updatedAt) {
    console.log(formatKeyValue("Atualizado em", summary.updatedAt));
  }

  const problematicEntries = summary.entries.filter(
    (entry) => entry.status !== "downloaded",
  );
  if (problematicEntries.length > 0) {
    console.log(theme.warningLabel("ARQUIVOS"));
    for (const entry of problematicEntries.slice(0, 20)) {
      const sizeLabel =
        entry.localSizeInBytes === undefined
          ? "sem bytes locais"
          : formatBytes(entry.localSizeInBytes);
      console.log(
        `  ${theme.yellow("•")} ${entry.fileName} (${entry.status}, ${sizeLabel})`,
      );
    }

    if (problematicEntries.length > 20) {
      console.log(
        `  ${theme.muted(`... ${problematicEntries.length - 20} arquivo(s) incompleto(s) adicional(is) omitido(s) da saída do terminal`)}`,
      );
    }
  }

  printWarnings(summary.warnings);
  console.log(
    `${theme.muted("Arquivo de log:")} ${resolveLogFilePath(logFilePath)}`,
  );
}

export function printFederalRevenueCleanSummary(
  summary: FederalRevenueCleanSummary,
  logFilePath: string,
): void {
  console.log(theme.successLabel("RECEITA FEDERAL"), "Limpeza concluída.");
  console.log(formatKeyValue("Referência", summary.reference));
  console.log(formatKeyValue("Modo de seleção", summary.selectionMode));
  console.log(formatKeyValue("Modo", summary.mode));
  console.log(formatKeyValue("Caminho de saída", summary.outputPath));
  console.log(formatKeyValue("Manifesto", summary.manifestPath));
  console.log(formatKeyValue("Arquivos removidos", summary.removedFiles));
  console.log(
    formatKeyValue("Bytes removidos", formatBytes(summary.removedBytes)),
  );

  if (summary.removedPaths.length > 0) {
    console.log(theme.infoLabel("REMOVIDOS"));
    for (const removedPath of summary.removedPaths.slice(0, 20)) {
      console.log(`  ${theme.blue("•")} ${removedPath}`);
    }

    if (summary.removedPaths.length > 20) {
      console.log(
        `  ${theme.muted(`... ${summary.removedPaths.length - 20} caminho(s) adicional(is) omitido(s) da saída do terminal`)}`,
      );
    }
  }

  printWarnings(summary.warnings);
  console.log(
    `${theme.muted("Arquivo de log:")} ${resolveLogFilePath(logFilePath)}`,
  );
}

export function printFederalRevenueSyncSummary(
  summary: FederalRevenueSyncSummary,
  logFilePath: string,
): void {
  console.log(
    theme.successLabel("RECEITA FEDERAL"),
    "Sync remoto completo concluído.",
  );
  console.log(formatKeyValue("Referência", summary.reference));
  console.log(
    formatKeyValue("Caminho de download", summary.download.outputPath),
  );
  console.log(
    formatKeyValue("Caminho extraído", summary.extraction.outputPath),
  );
  console.log(
    formatKeyValue("Caminho sanitizado", summary.sanitization.outputPath),
  );
  console.log(
    formatKeyValue("Banco de destino", summary.import.targetDatabase),
  );
  console.log(formatKeyValue("Arquivos ZIP", summary.download.filesFound));
  console.log(
    formatKeyValue(
      "Arquivos extraídos",
      summary.extraction.extractedArchives.length,
    ),
  );
  console.log(
    formatKeyValue("Arquivos sanitizados", summary.sanitization.processedFiles),
  );
  console.log(
    formatKeyValue("Arquivos importados", summary.import.importedFiles),
  );
  console.log(
    formatKeyValue(
      "Linhas confirmadas",
      formatCount(summary.import.processedRows),
    ),
  );
  console.log(
    formatKeyValue(
      "Linhas em quarentena",
      formatCount(summary.import.quarantinedRows),
    ),
  );

  printNotes(summary.warnings);
  console.log(
    `${theme.muted("Arquivo de log:")} ${resolveLogFilePath(logFilePath)}`,
  );
  console.log(
    `${theme.muted("Log de progresso da importação:")} ${resolveLogFilePath(summary.import.progressLogPath)}`,
  );
}

export function printPostgresCsvExportSummary(
  summary: PostgresCsvExportSummary,
  logFilePath: string,
): void {
  console.log(
    theme.successLabel("POSTGRES"),
    "Exportação de CSV pronto para o PostgreSQL concluída.",
  );
  console.log(formatKeyValue("Caminho de entrada", summary.inputPath));
  console.log(formatKeyValue("Caminho validado", summary.validatedPath));
  console.log(formatKeyValue("Caminho de saída", summary.outputPath));
  console.log(formatKeyValue("Script gerado", summary.scriptPath));
  console.log(formatKeyValue("Manifesto", summary.manifestPath));
  console.log(formatKeyValue("Arquivos exportados", summary.totalFiles));
  console.log(
    formatKeyValue("Linhas exportadas", formatCount(summary.totalRows)),
  );

  if (summary.datasets.length > 0) {
    console.log(theme.infoLabel("CONJUNTOS"));
    for (const dataset of summary.datasets) {
      console.log(
        `  ${theme.blue("•")} ${dataset.dataset}: ${dataset.files} arquivo(s), ${formatCount(dataset.rows)} linha(s)`,
      );
    }
  }

  printWarnings(summary.warnings);
  if (summary.nextStep) {
    console.log(`${theme.infoLabel("PRÓXIMO")} ${summary.nextStep}`);
  }

  console.log(
    `${theme.muted("Arquivo de log:")} ${resolveLogFilePath(logFilePath)}`,
  );
}

export function printPostgresDirectScriptSummary(
  summary: PostgresDirectScriptSummary,
  logFilePath: string,
): void {
  console.log(
    theme.successLabel("POSTGRES"),
    "Script de importação direta do PostgreSQL gerado.",
  );
  console.log(formatKeyValue("Caminho de entrada", summary.inputPath));
  console.log(formatKeyValue("Caminho validado", summary.validatedPath));
  console.log(formatKeyValue("Caminho de saída", summary.outputPath));
  console.log(formatKeyValue("Script gerado", summary.scriptPath));
  console.log(formatKeyValue("Manifesto", summary.manifestPath));
  console.log(formatKeyValue("Encoding de origem", summary.sourceEncoding));
  console.log(formatKeyValue("Modo de transação", summary.transactionMode));
  console.log(
    formatKeyValue("Arquivos SQL gerados", summary.scriptFiles.length),
  );
  console.log(
    formatKeyValue(
      "Etapas incluídas",
      summary.steps
        .filter((step) => step.included)
        .map((step) => step.name)
        .join(", "),
    ),
  );
  console.log(formatKeyValue("Arquivos de origem", summary.totalFiles));
  console.log(
    formatKeyValue("Bytes de origem", formatBytes(summary.totalBytes)),
  );

  if (summary.datasets.length > 0) {
    console.log(theme.infoLabel("CONJUNTOS"));
    for (const dataset of summary.datasets) {
      console.log(
        `  ${theme.blue("•")} ${dataset.dataset}: ${dataset.files} arquivo(s), ${formatBytes(dataset.totalBytes)}`,
      );
    }
  }

  printWarnings(summary.warnings);
  if (summary.nextStep) {
    console.log(`${theme.infoLabel("PRÓXIMO")} ${summary.nextStep}`);
  }

  console.log(
    `${theme.muted("Arquivo de log:")} ${resolveLogFilePath(logFilePath)}`,
  );
}
