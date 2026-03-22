import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

export async function ensureDirectory(filePath: string): Promise<void> {
  await mkdir(dirname(filePath), { recursive: true });
}

export async function safeReadText(
  filePath: string,
): Promise<string | undefined> {
  try {
    return await readFile(filePath, "utf8");
  } catch {
    return undefined;
  }
}

export async function safeWriteText(
  filePath: string,
  content: string,
): Promise<void> {
  await ensureDirectory(filePath);
  await writeFile(filePath, content, "utf8");
}
