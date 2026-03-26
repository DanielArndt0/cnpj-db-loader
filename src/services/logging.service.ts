import { appendFile, mkdir, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import {
  buildStructuredLogEntry,
  serializeErrorDetails,
} from "./logging/entry.js";
import type {
  LogLevel,
  LogStatus,
  StructuredLogEntry,
} from "./logging/types.js";

const DEFAULT_APP_DIRECTORY_NAME = ".cnpjdbloader";
const DEFAULT_LOGS_DIRECTORY_NAME = "logs";

type WriteLogOptions = {
  baseDirectory?: string;
  event?: string;
  level?: LogLevel;
  status?: LogStatus;
  message?: string;
};

function sanitizeSegment(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function createTimestamp(): string {
  const now = new Date();
  const parts = [
    now.getFullYear(),
    String(now.getMonth() + 1).padStart(2, "0"),
    String(now.getDate()).padStart(2, "0"),
    String(now.getHours()).padStart(2, "0"),
    String(now.getMinutes()).padStart(2, "0"),
    String(now.getSeconds()).padStart(2, "0"),
  ];

  return `${parts[0]}${parts[1]}${parts[2]}-${parts[3]}${parts[4]}${parts[5]}`;
}

export function getUserAppDirectoryPath(): string {
  return path.join(os.homedir(), DEFAULT_APP_DIRECTORY_NAME);
}

export function getLogsDirectoryPath(
  baseDirectory = getUserAppDirectoryPath(),
): string {
  return path.join(baseDirectory, DEFAULT_LOGS_DIRECTORY_NAME);
}

async function ensureLogsDirectory(baseDirectory?: string): Promise<string> {
  const logsDirectory = getLogsDirectoryPath(baseDirectory);
  await mkdir(logsDirectory, { recursive: true });
  return logsDirectory;
}

function buildCommandLogEntry(
  commandName: string,
  payload: unknown,
  options?: Omit<WriteLogOptions, "baseDirectory">,
): StructuredLogEntry {
  return buildStructuredLogEntry({
    payload,
    defaultEvent: options?.event ?? "command_completed",
    defaultLevel: options?.level ?? "info",
    command: commandName,
    status: options?.status ?? "success",
    ...(options?.message ? { message: options.message } : {}),
  });
}

export async function writeCommandLog(
  commandName: string,
  payload: unknown,
  baseDirectoryOrOptions?: string | WriteLogOptions,
  maybeOptions?: Omit<WriteLogOptions, "baseDirectory">,
): Promise<string> {
  const options: WriteLogOptions =
    typeof baseDirectoryOrOptions === "string"
      ? { ...maybeOptions, baseDirectory: baseDirectoryOrOptions }
      : (baseDirectoryOrOptions ?? {});

  const logsDirectory = await ensureLogsDirectory(options.baseDirectory);
  const fileName = `${createTimestamp()}-${sanitizeSegment(commandName)}.json`;
  const filePath = path.join(logsDirectory, fileName);
  const entry = buildCommandLogEntry(commandName, payload, options);

  await writeFile(filePath, `${JSON.stringify(entry, null, 2)}\n`, "utf8");
  return filePath;
}

export async function writeCommandFailureLog(
  commandName: string,
  error: unknown,
  input?: {
    argv?: string[];
    context?: Record<string, unknown>;
    baseDirectory?: string;
    fatal?: boolean;
  },
): Promise<string> {
  return writeCommandLog(
    commandName,
    {
      error: serializeErrorDetails(error),
      ...(input?.argv ? { argv: input.argv } : {}),
      ...(input?.context ? { context: input.context } : {}),
    },
    {
      ...(input?.baseDirectory ? { baseDirectory: input.baseDirectory } : {}),
      event: "command_failed",
      level: input?.fatal ? "fatal" : "error",
      status: "failure",
      message: "Command execution failed.",
    },
  );
}

export async function createJsonLinesLog(
  commandName: string,
  baseDirectory?: string,
): Promise<string> {
  const logsDirectory = await ensureLogsDirectory(baseDirectory);
  const fileName = `${createTimestamp()}-${sanitizeSegment(commandName)}.jsonl`;
  const filePath = path.join(logsDirectory, fileName);
  await writeFile(filePath, "", "utf8");
  return filePath;
}

export async function appendJsonLinesLog(
  filePath: string,
  payload: unknown,
  options?: Omit<WriteLogOptions, "baseDirectory" | "status"> & {
    command?: string;
    status?: LogStatus;
  },
): Promise<void> {
  const entry = buildStructuredLogEntry({
    payload,
    defaultEvent: options?.event ?? "log_entry",
    defaultLevel: options?.level ?? "info",
    ...(options?.command ? { command: options.command } : {}),
    ...(options?.status ? { status: options.status } : {}),
    ...(options?.message ? { message: options.message } : {}),
  });

  await appendFile(filePath, `${JSON.stringify(entry)}\n`, "utf8");
}

export type { LogLevel, LogStatus, StructuredLogEntry };
