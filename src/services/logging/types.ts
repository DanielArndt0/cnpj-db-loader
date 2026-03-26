export const LOG_LEVELS = [
  "trace",
  "debug",
  "info",
  "warning",
  "error",
  "critical",
  "fatal",
] as const;

export type LogLevel = (typeof LOG_LEVELS)[number];

export const LOG_STATUSES = ["success", "failure"] as const;

export type LogStatus = (typeof LOG_STATUSES)[number];

export type LogRecord = Record<string, unknown>;

export type StructuredLogEntry = LogRecord & {
  timestamp: string;
  level: LogLevel;
  severity: LogLevel;
  event: string;
  kind: string;
  command?: string;
  status?: LogStatus;
  message?: string;
};
