import path from "node:path";

import { theme } from "../theme.js";

export function resolveLogFilePath(logFilePath: string): string {
  return path.resolve(logFilePath);
}

export function formatKeyValue(label: string, value: string | number): string {
  return `${theme.muted(`- ${label}:`)} ${value}`;
}

export function printWarnings(warnings: string[]): void {
  if (warnings.length === 0) {
    return;
  }

  console.log(theme.warningLabel("WARNINGS"));
  for (const warning of warnings) {
    console.log(`  ${theme.yellow("•")} ${warning}`);
  }
}

export function printErrors(errors: string[]): void {
  if (errors.length === 0) {
    return;
  }

  console.log(theme.errorLabel("ERRORS"));
  for (const error of errors) {
    console.log(`  ${theme.red("•")} ${error}`);
  }
}

export function formatBytes(value: number): string {
  if (value < 1024) {
    return `${value} B`;
  }

  const units = ["KB", "MB", "GB", "TB"];
  let currentValue = value / 1024;
  let unitIndex = 0;

  while (currentValue >= 1024 && unitIndex < units.length - 1) {
    currentValue /= 1024;
    unitIndex += 1;
  }

  return `${currentValue.toFixed(currentValue >= 100 ? 0 : currentValue >= 10 ? 1 : 2)} ${units[unitIndex]}`;
}

export function formatCount(value: number): string {
  return new Intl.NumberFormat("en-US").format(value);
}

export function truncateMiddle(value: string, maxLength: number): string {
  if (value.length <= maxLength) {
    return value;
  }

  const edgeLength = Math.max(6, Math.floor((maxLength - 3) / 2));
  return `${value.slice(0, edgeLength)}...${value.slice(value.length - edgeLength)}`;
}
