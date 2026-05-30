import {
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  writeFile,
} from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { normalizeSanitizeSourceEncoding } from "../../services/sanitize/encoding.js";
import { sanitizeDatasetFile } from "../../services/sanitize/runner.js";
import type { SanitizeFilePlan } from "../../services/sanitize/types.js";

const temporaryDirectories: string[] = [];

async function createTemporaryDirectory(): Promise<string> {
  const directory = await mkdtemp(path.join(os.tmpdir(), "cnpj-sanitize-"));
  temporaryDirectories.push(directory);
  return directory;
}

function createPlan(
  inputPath: string,
  outputPath: string,
  fileSize: number,
): SanitizeFilePlan {
  return {
    dataset: "cnaes",
    relativePath: path.basename(inputPath),
    absolutePath: inputPath,
    outputPath,
    displayPath: inputPath,
    fileSize,
  };
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

describe("sanitizeDatasetFile", () => {
  it("normalizes ISO-8859-1 source bytes into validated UTF-8 output", async () => {
    const directory = await createTemporaryDirectory();
    const inputPath = path.join(directory, "F.K03200$Z.D60509.CNAECSV");
    const outputPath = path.join(
      directory,
      "sanitized",
      path.basename(inputPath),
    );
    const sourceText =
      '"0111399";"Cultivo de outros cereais não especificados anteriormente"\n' +
      '"0112101";"Cultivo de algodão herbáceo"\n' +
      '"0113000";"Cultivo de cana-de-açúcar"\n';
    const sourceBytes = Buffer.concat([
      Buffer.from(sourceText, "latin1"),
      Buffer.from([0x00]),
    ]);

    await writeFile(inputPath, sourceBytes);

    const result = await sanitizeDatasetFile(
      createPlan(inputPath, outputPath, sourceBytes.length),
    );

    expect(normalizeSanitizeSourceEncoding(undefined)).toBe("LATIN1");
    expect(await readFile(outputPath, "utf8")).toBe(sourceText);
    expect(result.sourceEncoding).toBe("LATIN1");
    expect(result.nulBytesRemoved).toBe(1);
    expect(result.replacementCharactersFound).toBe(0);
    expect(result.replacementCharactersRemaining).toBe(0);
    expect(Buffer.from(await readFile(outputPath)).includes(0x00)).toBe(false);
  });

  it("keeps the previous destination file when strict UTF-8 validation fails", async () => {
    const directory = await createTemporaryDirectory();
    const inputPath = path.join(directory, "F.K03200$Z.D60509.CNAECSV");
    const outputDirectory = path.join(directory, "sanitized");
    const outputPath = path.join(outputDirectory, path.basename(inputPath));

    await writeFile(inputPath, '"0111399";"Cultivo � inválido"\n', "utf8");
    await mkdir(outputDirectory, { recursive: true });
    await writeFile(outputPath, "previous valid output\n", "utf8");

    await expect(
      sanitizeDatasetFile(
        createPlan(inputPath, outputPath, (await readFile(inputPath)).length),
        undefined,
        { sourceEncoding: "UTF8" },
      ),
    ).rejects.toThrow("Unicode replacement character");

    expect(await readFile(outputPath, "utf8")).toBe("previous valid output\n");
    expect(
      (await readdir(outputDirectory)).some((fileName) =>
        fileName.includes(".sanitizing-"),
      ),
    ).toBe(false);
  });

  it("rejects previously persisted UTF-8 replacement markers when reading LATIN1 source files", async () => {
    const directory = await createTemporaryDirectory();
    const inputPath = path.join(directory, "F.K03200$Z.D60509.CNAECSV");
    const outputDirectory = path.join(directory, "sanitized");
    const outputPath = path.join(outputDirectory, path.basename(inputPath));

    await writeFile(
      inputPath,
      Buffer.from([
        0x22, 0x30, 0x31, 0x31, 0x31, 0x33, 0x39, 0x39, 0x22, 0x3b, 0x22, 0x43,
        0x75, 0x6c, 0x74, 0x69, 0x76, 0x6f, 0x20, 0xef, 0xbf, 0xbd, 0x22, 0x0a,
      ]),
    );
    await mkdir(outputDirectory, { recursive: true });
    await writeFile(outputPath, "previous valid output\n", "utf8");

    await expect(
      sanitizeDatasetFile(
        createPlan(inputPath, outputPath, (await readFile(inputPath)).length),
      ),
    ).rejects.toThrow("replacement marker");

    expect(await readFile(outputPath, "utf8")).toBe("previous valid output\n");
  });

  it("allows replacement characters only when strict mode is explicitly disabled", async () => {
    const directory = await createTemporaryDirectory();
    const inputPath = path.join(directory, "F.K03200$Z.D60509.CNAECSV");
    const outputPath = path.join(
      directory,
      "sanitized",
      path.basename(inputPath),
    );

    await writeFile(inputPath, '"0111399";"Cultivo � inválido"\n', "utf8");

    const result = await sanitizeDatasetFile(
      createPlan(inputPath, outputPath, (await readFile(inputPath)).length),
      undefined,
      { sourceEncoding: "UTF8", strict: false },
    );

    expect(result.replacementCharactersFound).toBe(1);
    expect(result.replacementCharactersRemaining).toBe(1);
    expect(await readFile(outputPath, "utf8")).toContain("�");
  });
});
