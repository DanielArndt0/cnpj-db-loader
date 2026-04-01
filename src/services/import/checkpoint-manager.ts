import type { Client } from "pg";

import {
  ensureCheckpointTable,
  hydratePlanWithCheckpoints,
  markCheckpointFailed,
  readCheckpoint,
  writeCheckpoint,
} from "./checkpoints.js";

export {
  ensureCheckpointTable as ensureImportCheckpointTable,
  hydratePlanWithCheckpoints as hydrateImportPlanWithCheckpoints,
  markCheckpointFailed as markImportCheckpointFailed,
  readCheckpoint as readImportCheckpoint,
  writeCheckpoint as writeImportCheckpoint,
};

export async function ensureImportCheckpointSupport(
  client: Client,
): Promise<void> {
  await ensureCheckpointTable(client);
}
