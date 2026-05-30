import { randomUUID } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, rename, rm } from "node:fs/promises";
import path from "node:path";
import { StringDecoder } from "node:string_decoder";
import { finished } from "node:stream/promises";

import { ValidationError } from "../../core/errors/index.js";
import {
  normalizeSanitizeSourceEncoding,
  SanitizeEncodingNormalizer,
} from "./encoding.js";
import type { SanitizeFilePlan, SanitizedFileResult } from "./types.js";

async function writeUtf8(
  output: ReturnType<typeof createWriteStream>,
  value: string,
): Promise<void> {
  if (value.length === 0) {
    return;
  }

  if (!output.write(value, "utf8")) {
    await new Promise<void>((resolve, reject) => {
      output.once("drain", resolve);
      output.once("error", reject);
    });
  }
}

function countNewlines(value: string): number {
  let count = 0;

  for (let index = 0; index < value.length; index += 1) {
    if (value[index] === "\n") {
      count += 1;
    }
  }

  return count;
}

function countReplacementCharacters(value: string): number {
  let count = 0;

  for (const char of value) {
    if (char === "\ufffd") {
      count += 1;
    }
  }

  return count;
}

async function validateNormalizedUtf8File(filePath: string): Promise<number> {
  const decoder = new StringDecoder("utf8");
  const input = createReadStream(filePath);
  let replacementCharactersFound = 0;

  try {
    for await (const chunk of input) {
      const chunkBuffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      replacementCharactersFound += countReplacementCharacters(
        decoder.write(chunkBuffer),
      );
    }

    replacementCharactersFound += countReplacementCharacters(decoder.end());
  } finally {
    input.destroy();
  }

  return replacementCharactersFound;
}

async function replaceDestinationFile(
  temporaryPath: string,
  destinationPath: string,
): Promise<void> {
  const backupPath = `${destinationPath}.backup-${randomUUID()}`;
  let destinationWasBackedUp = false;

  try {
    await rename(destinationPath, backupPath);
    destinationWasBackedUp = true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
      throw error;
    }
  }

  try {
    await rename(temporaryPath, destinationPath);
  } catch (error) {
    if (destinationWasBackedUp) {
      await rename(backupPath, destinationPath).catch(() => undefined);
    }

    throw error;
  }

  if (destinationWasBackedUp) {
    await rm(backupPath, { force: true });
  }
}

function buildTemporaryOutputPath(outputPath: string): string {
  return path.join(
    path.dirname(outputPath),
    `.${path.basename(outputPath)}.sanitizing-${randomUUID()}.tmp`,
  );
}

export async function sanitizeDatasetFile(
  plan: SanitizeFilePlan,
  onChunk?: (update: {
    bytesProcessed: number;
    fileBytesProcessed: number;
    currentFileSize: number;
    processedRows: number;
    nulBytesRemoved: number;
    invalidBytesRemoved: number;
    controlCharsRemoved: number;
    replacementCharactersFound: number;
  }) => void,
  options: {
    sourceEncoding?: string | undefined;
    strict?: boolean | undefined;
  } = {},
): Promise<SanitizedFileResult> {
  await mkdir(path.dirname(plan.outputPath), { recursive: true });

  const sourceEncoding = normalizeSanitizeSourceEncoding(
    options.sourceEncoding,
  );
  const strict = options.strict ?? true;
  const normalizer = new SanitizeEncodingNormalizer(sourceEncoding);
  const temporaryOutputPath = buildTemporaryOutputPath(plan.outputPath);
  const input = createReadStream(plan.absolutePath);
  const output = createWriteStream(temporaryOutputPath, { encoding: "utf8" });

  let totalBytesRead = 0;
  let totalBytesWritten = 0;
  let nulBytesRemoved = 0;
  let invalidBytesRemoved = 0;
  let controlCharsRemoved = 0;
  let replacementCharactersFound = 0;
  let lineCount = 0;
  let sawAnyCharacter = false;
  let lastCharacterWasNewline = false;

  const processText = async (text: string): Promise<void> => {
    if (text.length === 0) {
      return;
    }

    sawAnyCharacter = true;
    lineCount += countNewlines(text);
    lastCharacterWasNewline = text.endsWith("\n");
    totalBytesWritten += Buffer.byteLength(text, "utf8");
    await writeUtf8(output, text);
  };

  try {
    for await (const chunk of input) {
      const chunkBuffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      totalBytesRead += chunkBuffer.length;

      const normalized = normalizer.normalizeChunk(chunkBuffer);
      nulBytesRemoved += normalized.nulBytesRemoved;
      invalidBytesRemoved += normalized.invalidBytesRemoved;
      controlCharsRemoved += normalized.controlCharsRemoved;
      replacementCharactersFound += normalized.replacementCharactersFound;
      await processText(normalized.text);

      onChunk?.({
        bytesProcessed: chunkBuffer.length,
        fileBytesProcessed: totalBytesRead,
        currentFileSize: plan.fileSize,
        processedRows: lineCount,
        nulBytesRemoved,
        invalidBytesRemoved,
        controlCharsRemoved,
        replacementCharactersFound,
      });
    }

    const flushed = normalizer.flush();
    nulBytesRemoved += flushed.nulBytesRemoved;
    invalidBytesRemoved += flushed.invalidBytesRemoved;
    controlCharsRemoved += flushed.controlCharsRemoved;
    replacementCharactersFound += flushed.replacementCharactersFound;
    await processText(flushed.text);

    if (sawAnyCharacter && !lastCharacterWasNewline) {
      lineCount += 1;
    }

    output.end();
    await finished(output);

    const replacementCharactersRemaining =
      await validateNormalizedUtf8File(temporaryOutputPath);

    if (
      strict &&
      (replacementCharactersFound > 0 || replacementCharactersRemaining > 0)
    ) {
      throw new ValidationError(
        `Sanitized output validation failed for ${plan.displayPath}. Found ${replacementCharactersFound} replacement marker(s) in source decoding and ${replacementCharactersRemaining} Unicode replacement character(s) in normalized output. Verify the source encoding or source file before importing this dataset.`,
      );
    }

    await replaceDestinationFile(temporaryOutputPath, plan.outputPath);

    return {
      plan,
      totalBytesRead,
      totalBytesWritten,
      sourceEncoding,
      nulBytesRemoved,
      invalidBytesRemoved,
      controlCharsRemoved,
      replacementCharactersFound,
      replacementCharactersRemaining,
      lineCount,
      changed:
        nulBytesRemoved > 0 ||
        invalidBytesRemoved > 0 ||
        controlCharsRemoved > 0 ||
        replacementCharactersFound > 0 ||
        totalBytesRead !== totalBytesWritten,
    };
  } catch (error) {
    input.destroy();
    output.destroy();
    await rm(temporaryOutputPath, { force: true });
    throw error;
  } finally {
    input.destroy();
  }
}
