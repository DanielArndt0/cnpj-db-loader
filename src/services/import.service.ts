import { ValidationError } from "../core/errors/index.js";
import { resolveDatabaseUrl } from "./database.service.js";
import { inspectFiles } from "./inspect.service.js";
import { validateInputDirectory } from "./validate.service.js";
import {
  runImportLoadPipeline,
  runImportMaterializationPipeline,
  runImportPipeline,
} from "./import/runner.js";
import {
  maskDatabaseLabel,
  isImportDatasetType,
  type ImportOptions,
  type ImportSummary,
} from "./import/types.js";

export type {
  ImportCheckpointRecord,
  ImportCheckpointStatus,
  ImportDatasetPlan,
  ImportDatasetType,
  ImportFilePlan,
  ImportOptions,
  ImportPerformanceSummary,
  ImportPlanRecord,
  ImportPhaseStatus,
  ImportProgressEvent,
  ImportProgressListener,
  ImportSchemaCapabilities,
  ImportSummary,
} from "./import/types.js";

function validateRequestedDataset(dataset: string | undefined): void {
  if (dataset && !isImportDatasetType(dataset)) {
    throw new ValidationError(`Unsupported dataset type: ${dataset}.`);
  }
}

async function prepareImportInput(
  inputPath: string,
  options: ImportOptions,
): Promise<{
  inputPath: string;
  validatedPath: string;
  inspection: Awaited<ReturnType<typeof inspectFiles>>;
  dbUrl: string;
  targetDatabase: string;
  options: ImportOptions;
}> {
  validateRequestedDataset(options.dataset);

  const validation = await validateInputDirectory(inputPath);
  if (!validation.ok) {
    throw new ValidationError(
      `The input directory is not ready for import. ${validation.errors.join(" ")}`,
    );
  }

  const inspection = await inspectFiles(validation.validatedPath);
  const dbUrl = await resolveDatabaseUrl(options.dbUrl);

  return {
    inputPath,
    validatedPath: validation.validatedPath,
    inspection,
    dbUrl,
    options,
    targetDatabase: maskDatabaseLabel(dbUrl),
  };
}

export async function importDataToDatabase(
  inputPath: string,
  options: ImportOptions = {},
): Promise<ImportSummary> {
  const prepared = await prepareImportInput(inputPath, options);
  return runImportPipeline(prepared);
}

export async function loadImportDataToStaging(
  inputPath: string,
  options: ImportOptions = {},
): Promise<ImportSummary> {
  const prepared = await prepareImportInput(inputPath, options);
  return runImportLoadPipeline(prepared);
}

export async function materializeImportedData(
  inputPath: string,
  options: ImportOptions = {},
): Promise<ImportSummary> {
  const prepared = await prepareImportInput(inputPath, options);
  return runImportMaterializationPipeline(prepared);
}
