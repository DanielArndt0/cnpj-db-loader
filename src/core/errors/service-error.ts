import { AppError } from "./app-error.js";

export class ServiceError extends AppError {
  constructor(message: string, details?: unknown) {
    super(message, "SERVICE_ERROR", details);
    this.name = "ServiceError";
  }
}
