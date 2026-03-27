import { ValidationError } from "../core/errors/index.js";
import { resolveDbUrl } from "./db.service.js";
import { inspectFiles } from "./inspect.service.js";
import { validateInputDirectory } from "./validate.service.js";
import {
  runImportLoadPipeline,
  runImportMaterializationPipeline,
  runImportPipeline,
} from "./import/runner.js";
import { resetStagingTablesForFreshPlan } from "./import/staging-schema.js";
import {
  ensureMaterializationCheckpointTable,
  resetMaterializationCheckpoints,
} from "./import/materialization-checkpoints.js";
import {
  ensureImportPlanTables,
  readLatestImportPlanForValidatedPath,
} from "./import/plan-store.js";
import {
  maskDatabaseLabel,
  isImportDatasetType,
  type ImportOptions,
  type ImportSummary,
  type ImportDatasetType,
} from "./import/types.js";
import { Client } from "pg";

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
  const dbUrl = await resolveDbUrl(options.dbUrl);

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

export async function cleanupImportStaging(
  options: {
    dbUrl?: string;
    dataset?: ImportDatasetType | undefined;
    validatedPath?: string;
  } = {},
): Promise<{
  targetDatabase: string;
  truncatedTables: string[];
  clearedPlans: number;
}> {
  validateRequestedDataset(options.dataset);
  const dbUrl = await resolveDbUrl(options.dbUrl);
  const targetDatabase = maskDatabaseLabel(dbUrl);
  const client = new Client({ connectionString: dbUrl });

  await client.connect();
  try {
    const datasets: ImportDatasetType[] = options.dataset
      ? [options.dataset]
      : ["companies", "establishments", "partners", "simples_options"];

    const truncatedTables = await resetStagingTablesForFreshPlan(
      client,
      datasets,
    );
    await ensureImportPlanTables(client);
    await ensureMaterializationCheckpointTable(client);

    let clearedPlans = 0;
    if (options.validatedPath) {
      const plan = await readLatestImportPlanForValidatedPath(
        client,
        options.validatedPath,
        targetDatabase,
      );
      if (plan) {
        await resetMaterializationCheckpoints(client, plan.plan.id);
        clearedPlans = 1;
      }
    }

    return { targetDatabase, truncatedTables, clearedPlans };
  } finally {
    await client.end().catch(() => undefined);
  }
}
