import { ValidationError } from "../core/errors/index.js";
import { resolveDbUrl } from "./db.service.js";
import { inspectFiles } from "./inspect.service.js";
import { validateInputDirectory } from "./validate.service.js";
import { runImportPipeline } from "./import/runner.js";
import {
  isImportDatasetType,
  maskDatabaseLabel,
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
  ImportProgressEvent,
  ImportProgressListener,
  ImportSchemaCapabilities,
  ImportSummary,
} from "./import/types.js";

export async function importDataToDatabase(
  inputPath: string,
  options: ImportOptions = {},
): Promise<ImportSummary> {
  if (options.dataset && !isImportDatasetType(options.dataset)) {
    throw new ValidationError(`Unsupported dataset type: ${options.dataset}.`);
  }

  const validation = await validateInputDirectory(inputPath);
  if (!validation.ok) {
    throw new ValidationError(
      `The input directory is not ready for import. ${validation.errors.join(" ")}`,
    );
  }

  const inspection = await inspectFiles(validation.validatedPath);
  const dbUrl = await resolveDbUrl(options.dbUrl);

  return runImportPipeline({
    inputPath,
    validatedPath: validation.validatedPath,
    inspection,
    dbUrl,
    options,
    targetDatabase: maskDatabaseLabel(dbUrl),
  });
}
