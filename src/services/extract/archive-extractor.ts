import { spawn } from "node:child_process";
import { rm } from "node:fs/promises";

import { path7z } from "7zip-bin-full";

const MAX_PROCESS_OUTPUT_LENGTH = 32_000;

export type ArchiveExtractionOptions = {
  archivePath: string;
  destinationPath: string;
};

export type ArchiveExtractionResult = {
  engine: "7zip";
};

function appendProcessOutput(current: string, chunk: Buffer): string {
  const next = current + chunk.toString("utf8");
  if (next.length <= MAX_PROCESS_OUTPUT_LENGTH) {
    return next;
  }

  return next.slice(next.length - MAX_PROCESS_OUTPUT_LENGTH);
}

function describeFailure(stdout: string, stderr: string): string {
  const details = `${stderr}\n${stdout}`.trim();
  return details.length > 0
    ? details
    : "7-Zip extraction failed without output.";
}

export async function extractArchiveWithSevenZip(
  options: ArchiveExtractionOptions,
): Promise<ArchiveExtractionResult> {
  await rm(options.destinationPath, { recursive: true, force: true });

  await new Promise<void>((resolve, reject) => {
    const child = spawn(
      path7z,
      [
        "x",
        "-y",
        "-aoa",
        "-bd",
        "-bb0",
        `-o${options.destinationPath}`,
        "--",
        options.archivePath,
      ],
      {
        windowsHide: true,
        stdio: ["ignore", "pipe", "pipe"],
      },
    );

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk: Buffer) => {
      stdout = appendProcessOutput(stdout, chunk);
    });

    child.stderr.on("data", (chunk: Buffer) => {
      stderr = appendProcessOutput(stderr, chunk);
    });

    child.once("error", (error) => {
      reject(
        new Error(
          `Unable to start the bundled 7-Zip extractor: ${error.message}`,
        ),
      );
    });

    child.once("close", (code, signal) => {
      if (code === 0) {
        resolve();
        return;
      }

      const signalSuffix = signal ? ` Signal: ${signal}.` : "";
      reject(
        new Error(
          `7-Zip extraction failed with exit code ${code ?? "unknown"}.${signalSuffix} ${describeFailure(stdout, stderr)}`,
        ),
      );
    });
  });

  return { engine: "7zip" };
}
