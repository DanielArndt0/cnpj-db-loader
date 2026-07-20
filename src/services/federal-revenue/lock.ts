import { mkdir, open, readFile, unlink } from "node:fs/promises";
import { randomUUID } from "node:crypto";

import { ValidationError } from "../../core/errors/index.js";
import {
  getFederalRevenueControlDirectory,
  getFederalRevenueSyncLockPath,
} from "./manifest.js";
import type {
  FederalRevenueLockFile,
  FederalRevenueSyncLockOptions,
} from "./types.js";

async function readLock(
  lockPath: string,
): Promise<FederalRevenueLockFile | undefined> {
  try {
    return JSON.parse(
      await readFile(lockPath, "utf8"),
    ) as FederalRevenueLockFile;
  } catch {
    return undefined;
  }
}

async function removeLock(lockPath: string): Promise<void> {
  try {
    await unlink(lockPath);
  } catch {
    // Ignore missing lock files. The next exclusive write still validates the lock state.
  }
}

export async function withFederalRevenueSyncLock<T>(
  input: {
    reference: string;
    outputPath: string;
    options?: FederalRevenueSyncLockOptions | undefined;
  },
  callback: () => Promise<T>,
): Promise<T> {
  const lockPath = getFederalRevenueSyncLockPath(input.outputPath);
  const token = randomUUID();
  const lockFile: FederalRevenueLockFile = {
    reference: input.reference,
    outputPath: input.outputPath,
    lockPath,
    pid: process.pid,
    token,
    startedAt: new Date().toISOString(),
  };

  await mkdir(getFederalRevenueControlDirectory(input.outputPath), {
    recursive: true,
  });

  if (input.options?.forceLock) {
    await removeLock(lockPath);
  }

  let handle: Awaited<ReturnType<typeof open>> | undefined;

  try {
    handle = await open(lockPath, "wx");
    await handle.writeFile(`${JSON.stringify(lockFile, null, 2)}\n`, "utf8");
  } catch {
    const existingLock = await readLock(lockPath);
    throw new ValidationError(
      `O sync da Receita Federal já está em execução para ${existingLock?.reference ?? input.reference}. Use --force-lock apenas se o processo anterior não estiver mais ativo.`,
      {
        lockPath,
        pid: existingLock?.pid,
        startedAt: existingLock?.startedAt,
      },
    );
  } finally {
    await handle?.close();
  }

  try {
    return await callback();
  } finally {
    const currentLock = await readLock(lockPath);
    if (currentLock?.token === token) {
      await removeLock(lockPath);
    }
  }
}
