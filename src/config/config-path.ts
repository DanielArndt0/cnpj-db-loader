import os from "node:os";
import path from "node:path";

export function getConfigFilePath(): string {
  if (process.platform === "win32") {
    const appData =
      process.env.APPDATA ?? path.join(os.homedir(), "AppData", "Roaming");
    return path.join(appData, "cnpj-db-loader", "config.json");
  }

  return path.join(os.homedir(), ".config", "cnpj-db-loader", "config.json");
}
