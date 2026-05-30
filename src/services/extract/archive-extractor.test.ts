import { spawn } from "node:child_process";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { path7z } from "7zip-bin-full";
import { afterEach, describe, expect, it } from "vitest";

import { extractArchives } from "../extract.service.js";
import { inspectFiles } from "../inspect.service.js";
import { extractArchiveWithSevenZip } from "./archive-extractor.js";

const temporaryPaths: string[] = [];

async function createTemporaryDirectory(): Promise<string> {
  const temporaryPath = await mkdtemp(
    path.join(os.tmpdir(), "cnpj-db-loader-"),
  );
  temporaryPaths.push(temporaryPath);
  return temporaryPath;
}

async function runSevenZip(args: string[]): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const child = spawn(path7z, args, {
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let output = "";

    child.stdout.on("data", (chunk: Buffer) => {
      output += chunk.toString("utf8");
    });

    child.stderr.on("data", (chunk: Buffer) => {
      output += chunk.toString("utf8");
    });

    child.once("error", reject);
    child.once("close", (code) => {
      if (code === 0) {
        resolve();
        return;
      }

      reject(
        new Error(`7-Zip test command failed with code ${code}: ${output}`),
      );
    });
  });
}

afterEach(async () => {
  await Promise.all(
    temporaryPaths
      .splice(0)
      .map((temporaryPath) =>
        rm(temporaryPath, { recursive: true, force: true }),
      ),
  );
});

describe("extractArchiveWithSevenZip", () => {
  it("extracts a regular ZIP archive", async () => {
    const rootPath = await createTemporaryDirectory();
    const sourcePath = path.join(rootPath, "source");
    const archivePath = path.join(rootPath, "archive.zip");
    const destinationPath = path.join(rootPath, "destination");

    await mkdir(sourcePath, { recursive: true });
    await writeFile(path.join(sourcePath, "sample.txt"), "sample", "utf8");
    await runSevenZip([
      "a",
      "-tzip",
      archivePath,
      path.join(sourcePath, "sample.txt"),
    ]);

    await extractArchiveWithSevenZip({ archivePath, destinationPath });

    await expect(
      readFile(path.join(destinationPath, "sample.txt"), "utf8"),
    ).resolves.toBe("sample");
  });

  it("extracts a split ZIP archive from its first volume", async () => {
    const rootPath = await createTemporaryDirectory();
    const sourcePath = path.join(rootPath, "source");
    const archivePath = path.join(rootPath, "archive.zip");
    const firstVolumePath = `${archivePath}.001`;
    const destinationPath = path.join(rootPath, "destination");

    await mkdir(sourcePath, { recursive: true });
    await writeFile(
      path.join(sourcePath, "sample.txt"),
      "x".repeat(10_000),
      "utf8",
    );
    await runSevenZip([
      "a",
      "-tzip",
      "-mx=0",
      "-v1k",
      archivePath,
      path.join(sourcePath, "sample.txt"),
    ]);

    await extractArchiveWithSevenZip({
      archivePath: firstVolumePath,
      destinationPath,
    });

    await expect(
      readFile(path.join(destinationPath, "sample.txt"), "utf8"),
    ).resolves.toBe("x".repeat(10_000));
  });

  it("discovers and extracts a split ZIP archive from its first volume", async () => {
    const rootPath = await createTemporaryDirectory();
    const sourcePath = path.join(rootPath, "source");
    const archivePath = path.join(rootPath, "archive.zip");
    const extractionPath = path.join(rootPath, "extracted");

    await mkdir(sourcePath, { recursive: true });
    await writeFile(
      path.join(sourcePath, "sample.txt"),
      "x".repeat(10_000),
      "utf8",
    );
    await runSevenZip([
      "a",
      "-tzip",
      "-mx=0",
      "-v1k",
      archivePath,
      path.join(sourcePath, "sample.txt"),
    ]);

    const inspection = await inspectFiles(rootPath);
    const summary = await extractArchives(rootPath, extractionPath);

    expect(inspection.detectedInputMode).toBe("zip-archives-only");
    expect(inspection.zipArchivesFound).toBe(1);
    expect(summary.zipFilesFound).toBe(1);
    expect(summary.failedArchives).toEqual([]);
    await expect(
      readFile(path.join(extractionPath, "archive", "sample.txt"), "utf8"),
    ).resolves.toBe("x".repeat(10_000));
  });
});
