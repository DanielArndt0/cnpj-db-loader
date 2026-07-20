import type {
  ImportDatasetPerformanceSummary,
  ImportDatasetPlan,
  ImportDatasetType,
  ImportPerformanceSummary,
} from "./types.js";

export type MutableDatasetPerformance = Omit<
  ImportDatasetPerformanceSummary,
  "rowsPerSecond" | "batchesPerMinute"
>;

export function calculateRowsPerSecond(
  rows: number,
  durationMs: number,
): number {
  if (rows <= 0 || durationMs <= 0) {
    return 0;
  }

  return rows / (durationMs / 1000);
}

export function calculateBatchesPerMinute(
  batches: number,
  durationMs: number,
): number {
  if (batches <= 0 || durationMs <= 0) {
    return 0;
  }

  return batches / (durationMs / 60000);
}

export function createDatasetPerformanceTracker(
  datasetPlan: ImportDatasetPlan,
  scanDurationMs: number,
): MutableDatasetPerformance {
  return {
    dataset: datasetPlan.dataset,
    files: datasetPlan.files.length,
    plannedRows: datasetPlan.totalRows,
    importedRows: 0,
    plannedBatches: datasetPlan.totalBatches,
    committedBatches: 0,
    resumedFiles: 0,
    skippedCompletedFiles: 0,
    retriedRows: 0,
    retriedBatches: 0,
    quarantinedRows: 0,
    scanDurationMs,
    importDurationMs: 0,
    insertDurationMs: 0,
    retryDurationMs: 0,
    quarantineDurationMs: 0,
    materializationDurationMs: 0,
  };
}

export function finalizeDatasetPerformance(
  tracker: MutableDatasetPerformance,
): ImportDatasetPerformanceSummary {
  return {
    ...tracker,
    rowsPerSecond: calculateRowsPerSecond(
      tracker.importedRows,
      tracker.importDurationMs,
    ),
    batchesPerMinute: calculateBatchesPerMinute(
      tracker.committedBatches,
      tracker.importDurationMs,
    ),
  };
}

export function buildImportPerformanceSummary(input: {
  planReused: boolean;
  totalDurationMs: number;
  scanDurationMs: number;
  executionDurationMs: number;
  lookupLoadDurationMs: number;
  executionRowsCommitted: number;
  executionBatchesCommitted: number;
  datasets: MutableDatasetPerformance[];
}): ImportPerformanceSummary {
  const finalizedDatasets = input.datasets.map(finalizeDatasetPerformance);

  return {
    planReused: input.planReused,
    totalDurationMs: input.totalDurationMs,
    scanDurationMs: input.scanDurationMs,
    executionDurationMs: input.executionDurationMs,
    lookupLoadDurationMs: input.lookupLoadDurationMs,
    insertDurationMs: finalizedDatasets.reduce(
      (sum, item) => sum + item.insertDurationMs,
      0,
    ),
    retryDurationMs: finalizedDatasets.reduce(
      (sum, item) => sum + item.retryDurationMs,
      0,
    ),
    quarantineDurationMs: finalizedDatasets.reduce(
      (sum, item) => sum + item.quarantineDurationMs,
      0,
    ),
    materializationDurationMs: finalizedDatasets.reduce(
      (sum, item) => sum + item.materializationDurationMs,
      0,
    ),
    rowsPerSecond: calculateRowsPerSecond(
      input.executionRowsCommitted,
      input.executionDurationMs,
    ),
    batchesPerMinute: calculateBatchesPerMinute(
      input.executionBatchesCommitted,
      input.executionDurationMs,
    ),
    datasets: finalizedDatasets,
  };
}

export function buildImportWarnings(): string[] {
  return [
    "O importador usa planejamento exato de arquivos, commits de lote com checkpoint e retomada por deslocamento de bytes. Se uma unidade de carga falha, reexecutar o mesmo comando retoma a partir do último checkpoint confirmado, em vez de reiniciar a carga completa.",
    "Os planos de importação são persistidos no banco e reutilizados para a mesma entrada validada, os mesmos arquivos de origem e o mesmo tamanho de lote de carga, de modo que importações retomadas não recontam linhas desnecessariamente.",
    "Grandes datasets agora chegam a tabelas de staging leves via COPY do PostgreSQL, com apenas normalização leve no hot path de escrita. A materialização no schema final simplificado mantém a primeira carga focada em persistência rápida, em vez de enriquecimento relacional antecipado.",
    "Quando um novo plano de importação começa, as tabelas de staging selecionadas são truncadas antes da carga, para que as cargas em massa de staging fiquem limpas e previsíveis. Planos retomados mantêm as linhas de staging que já correspondem aos checkpoints salvos.",
    "Linhas que falham no parsing, na normalização, no fallback de COPY ou nos inserts linha a linha são movidas para quarentena_importacao e a importação continua a partir da linha seguinte.",
    "O resumo da importação inclui métricas de tempo e vazão de referência para os caminhos de varredura, execução, escrita de staging, materialização, retry e quarentena, para que mudanças futuras de desempenho possam ser medidas contra uma referência estável.",
    "O tamanho do lote de carga define o tamanho da unidade de carga de staging, enquanto o tamanho do lote de materialização define quantas linhas de staging cada bloco de consolidação processa antes de salvar um checkpoint de materialização.",
  ];
}

export function summarizeImportedDatasets(
  datasets: ImportDatasetPlan[],
): Array<{
  dataset: ImportDatasetType;
  files: number;
  rows: number;
}> {
  return datasets.map((datasetPlan) => ({
    dataset: datasetPlan.dataset,
    files: datasetPlan.files.length,
    rows: datasetPlan.totalRows,
  }));
}
