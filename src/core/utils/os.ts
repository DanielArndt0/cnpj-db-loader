import os from "node:os";

export type SupportedOs = "windows" | "macos" | "linux" | "unknown";

export function detectOs(): SupportedOs {
  const platform = os.platform();

  if (platform === "win32") {
    return "windows";
  }

  if (platform === "darwin") {
    return "macos";
  }

  if (platform === "linux") {
    return "linux";
  }

  return "unknown";
}
