export class AppError extends Error {
  public readonly code: string;
  public readonly details?: unknown;

  constructor(message: string, code = "APP_ERROR", details?: unknown) {
    super(message);
    this.name = "AppError";
    this.code = code;
    this.details = details;
  }
}
