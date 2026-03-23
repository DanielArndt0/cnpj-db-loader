export type QuarantineStatsFilters = {
  dataset?: string;
  category?: string;
  stage?: string;
  retryable?: boolean;
  terminal?: boolean;
};

export type QuarantineListFilters = QuarantineStatsFilters & {
  limit: number;
  afterId?: number;
};

export type QuarantineStatsCount = {
  key: string;
  count: number;
};

export type QuarantineStatsSummary = {
  totalRows: number;
  retryableRows: number;
  terminalRows: number;
  rowsByDataset: QuarantineStatsCount[];
  rowsByCategory: QuarantineStatsCount[];
  rowsByStage: QuarantineStatsCount[];
  appliedFilters: QuarantineStatsFilters;
};

export type QuarantineListRow = {
  id: number;
  dataset: string;
  filePath: string;
  rowNumber: number | null;
  checkpointOffset: number | null;
  errorCode: string | null;
  errorCategory: string | null;
  errorStage: string | null;
  errorMessage: string;
  retryCount: number;
  canRetryLater: boolean;
  createdAt: string;
};

export type QuarantineListSummary = {
  rows: QuarantineListRow[];
  appliedFilters: QuarantineListFilters;
};

export type QuarantineRecord = {
  id: number;
  dataset: string;
  filePath: string;
  rowNumber: number | null;
  checkpointOffset: number | null;
  errorCode: string | null;
  errorCategory: string | null;
  errorStage: string | null;
  errorMessage: string;
  rawLine: string;
  parsedPayload: Record<string, unknown> | null;
  sanitizationsApplied: unknown[];
  retryCount: number;
  canRetryLater: boolean;
  createdAt: string;
};
