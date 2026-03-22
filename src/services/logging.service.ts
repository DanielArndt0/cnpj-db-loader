import { appendFile, mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

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

export async function writeCommandLog(
  commandName: string,
  payload: unknown,
  baseDirectory = process.cwd(),
): Promise<string> {
  const logsDirectory = path.join(baseDirectory, "logs");
  await mkdir(logsDirectory, { recursive: true });

  const fileName = `${createTimestamp()}-${sanitizeSegment(commandName)}.json`;
  const filePath = path.join(logsDirectory, fileName);

  await writeFile(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  return filePath;
}


export async function createJsonLinesLog(
  commandName: string,
  baseDirectory = process.cwd(),
): Promise<string> {
  const logsDirectory = path.join(baseDirectory, "logs");
  await mkdir(logsDirectory, { recursive: true });

  const fileName = `${createTimestamp()}-${sanitizeSegment(commandName)}.jsonl`;
  const filePath = path.join(logsDirectory, fileName);
  await writeFile(filePath, "", "utf8");
  return filePath;
}

export async function appendJsonLinesLog(
  filePath: string,
  payload: unknown,
): Promise<void> {
  await appendFile(filePath, `${JSON.stringify(payload)}
`, "utf8");
}
