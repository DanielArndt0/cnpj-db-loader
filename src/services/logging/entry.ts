import {
  LOG_LEVELS,
  LOG_STATUSES,
  type LogLevel,
  type LogRecord,
  type LogStatus,
  type StructuredLogEntry,
} from "./types.js";

type BuildStructuredLogEntryInput = {
  payload: unknown;
  defaultEvent: string;
  defaultLevel: LogLevel;
  command?: string;
  status?: LogStatus;
  message?: string;
};

const EVENT_LEVEL_MAP: Record<string, LogLevel> = {
  archive_complete: "info",
  archive_failed: "error",
  archive_start: "info",
  batch_committed: "debug",
  batch_retry_fallback: "warning",
  command_completed: "info",
  command_failed: "error",
  dataset_completed: "info",
  dataset_started: "info",
  file_failed: "error",
  file_metrics: "info",
  import_failed: "error",
  import_finished: "info",
  import_plan_ready: "info",
  import_plan_reused: "info",
  import_started: "info",
  log_entry: "info",
  preparing_progress: "debug",
  preparing_start: "info",
  row_quarantined: "warning",
};

function isRecord(value: unknown): value is LogRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isLogLevel(value: unknown): value is LogLevel {
  return typeof value === "string" && LOG_LEVELS.includes(value as LogLevel);
}

function isLogStatus(value: unknown): value is LogStatus {
  return typeof value === "string" && LOG_STATUSES.includes(value as LogStatus);
}

function readOptionalString(
  record: LogRecord,
  key: string,
): string | undefined {
  const value = record[key];
  return typeof value === "string" && value.trim() !== "" ? value : undefined;
}

function normalizeEventName(value: string): string {
  return value.trim().replace(/-/g, "_");
}

function resolveLogLevel(
  explicitLevel: LogLevel | undefined,
  event: string,
  fallbackLevel: LogLevel,
): LogLevel {
  if (explicitLevel) {
    return explicitLevel;
  }

  return EVENT_LEVEL_MAP[event] ?? fallbackLevel;
}

function buildPrimitivePayloadEntry(
  input: BuildStructuredLogEntryInput,
): StructuredLogEntry {
  const event = normalizeEventName(input.defaultEvent);
  const level = resolveLogLevel(undefined, event, input.defaultLevel);

  return {
    timestamp: new Date().toISOString(),
    level,
    severity: level,
    event,
    kind: event,
    ...(input.command ? { command: input.command } : {}),
    ...(input.status ? { status: input.status } : {}),
    ...(input.message ? { message: input.message } : {}),
    payload: input.payload,
  };
}

export function buildStructuredLogEntry(
  input: BuildStructuredLogEntryInput,
): StructuredLogEntry {
  if (!isRecord(input.payload)) {
    return buildPrimitivePayloadEntry(input);
  }

  const payload = { ...input.payload };
  const rawEvent =
    readOptionalString(payload, "event") ??
    readOptionalString(payload, "kind") ??
    input.defaultEvent;
  const event = normalizeEventName(rawEvent);
  const explicitLevel =
    (isLogLevel(payload.level) ? payload.level : undefined) ??
    (isLogLevel(payload.severity) ? payload.severity : undefined);
  const level = resolveLogLevel(explicitLevel, event, input.defaultLevel);
  const timestamp =
    readOptionalString(payload, "timestamp") ?? new Date().toISOString();
  const command = readOptionalString(payload, "command") ?? input.command;
  const status =
    (isLogStatus(payload.status) ? payload.status : undefined) ?? input.status;
  const message = readOptionalString(payload, "message") ?? input.message;
  const kind = normalizeEventName(readOptionalString(payload, "kind") ?? event);

  return {
    ...payload,
    timestamp,
    level,
    severity: level,
    event,
    kind,
    ...(command ? { command } : {}),
    ...(status ? { status } : {}),
    ...(message ? { message } : {}),
  };
}

export function serializeErrorDetails(error: unknown): LogRecord {
  if (error instanceof Error) {
    const details: LogRecord = {
      name: error.name,
      message: error.message,
    };

    if (typeof error.stack === "string" && error.stack.trim() !== "") {
      details.stack = error.stack;
    }

    const errorWithCode = error as Error & { code?: unknown; cause?: unknown };

    if (typeof errorWithCode.code === "string") {
      details.code = errorWithCode.code;
    }

    if (errorWithCode.cause !== undefined) {
      details.cause =
        errorWithCode.cause instanceof Error
          ? serializeErrorDetails(errorWithCode.cause)
          : errorWithCode.cause;
    }

    return details;
  }

  return {
    message: String(error),
  };
}
