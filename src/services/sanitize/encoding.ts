import iconv from "iconv-lite";

import { ValidationError } from "../../core/errors/index.js";

export type SanitizeSourceEncoding = "WIN1252" | "LATIN1" | "UTF8";

export type SanitizedTextChunk = {
  text: string;
  nulBytesRemoved: number;
  invalidBytesRemoved: number;
  controlCharsRemoved: number;
  replacementCharactersFound: number;
};

export function normalizeSanitizeSourceEncoding(
  value: string | undefined,
): SanitizeSourceEncoding {
  const normalized = (value ?? "LATIN1")
    .trim()
    .toUpperCase()
    .replace(/_/g, "-");

  switch (normalized) {
    case "WIN1252":
    case "WINDOWS-1252":
    case "CP1252":
      return "WIN1252";
    case "LATIN1":
    case "LATIN-1":
    case "ISO-8859-1":
    case "ISO8859-1":
      return "LATIN1";
    case "UTF8":
    case "UTF-8":
      return "UTF8";
    default:
      throw new ValidationError(
        `Unsupported sanitize source encoding: ${value}. Supported values: WIN1252, LATIN1, UTF8.`,
      );
  }
}

function decoderEncoding(sourceEncoding: SanitizeSourceEncoding): string {
  switch (sourceEncoding) {
    case "WIN1252":
      return "windows-1252";
    case "LATIN1":
      return "iso-8859-1";
    case "UTF8":
      return "utf8";
  }
}

function isAllowedControlCodePoint(codePoint: number): boolean {
  return codePoint === 0x09 || codePoint === 0x0a || codePoint === 0x0d;
}

function isProblematicControlCodePoint(codePoint: number): boolean {
  if (isAllowedControlCodePoint(codePoint)) {
    return false;
  }

  return (
    (codePoint >= 0x00 && codePoint <= 0x1f) ||
    codePoint === 0x7f ||
    (codePoint >= 0x80 && codePoint <= 0x9f) ||
    codePoint === 0xfeff
  );
}

function removeNulBytes(chunk: Buffer): {
  chunk: Buffer;
  nulBytesRemoved: number;
} {
  if (!chunk.includes(0x00)) {
    return { chunk, nulBytesRemoved: 0 };
  }

  const output = Buffer.allocUnsafe(chunk.length);
  let offset = 0;
  let nulBytesRemoved = 0;

  for (const byte of chunk) {
    if (byte === 0x00) {
      nulBytesRemoved += 1;
      continue;
    }

    output[offset] = byte;
    offset += 1;
  }

  return {
    chunk: output.subarray(0, offset),
    nulBytesRemoved,
  };
}

function sanitizeDecodedText(
  text: string,
  nulBytesRemoved: number,
): SanitizedTextChunk {
  const output: string[] = [];
  let controlCharsRemoved = 0;
  let replacementCharactersFound = 0;

  for (const char of text) {
    const codePoint = char.codePointAt(0)!;

    if (codePoint === 0xfffd) {
      replacementCharactersFound += 1;
      output.push(char);
      continue;
    }

    if (isProblematicControlCodePoint(codePoint)) {
      controlCharsRemoved += 1;
      continue;
    }

    output.push(char);
  }

  return {
    text: output.join(""),
    nulBytesRemoved,
    invalidBytesRemoved: 0,
    controlCharsRemoved,
    replacementCharactersFound,
  };
}

export class SanitizeEncodingNormalizer {
  private readonly decoder;
  private replacementByteSequenceCarry = Buffer.alloc(0);

  constructor(private readonly sourceEncoding: SanitizeSourceEncoding) {
    this.decoder = iconv.getDecoder(decoderEncoding(sourceEncoding));
  }

  normalizeChunk(chunk: Buffer): SanitizedTextChunk {
    const replacementByteSequencesFound =
      this.sourceEncoding === "UTF8"
        ? 0
        : this.countUtf8ReplacementByteSequences(chunk);
    const withoutNulBytes = removeNulBytes(chunk);
    const decoded = this.decoder.write(withoutNulBytes.chunk);
    const sanitized = sanitizeDecodedText(
      decoded,
      withoutNulBytes.nulBytesRemoved,
    );

    return {
      ...sanitized,
      replacementCharactersFound:
        sanitized.replacementCharactersFound + replacementByteSequencesFound,
    };
  }

  flush(): SanitizedTextChunk {
    const decoded = this.decoder.end() ?? "";
    return sanitizeDecodedText(decoded, 0);
  }

  private countUtf8ReplacementByteSequences(chunk: Buffer): number {
    const searchable = Buffer.concat([
      this.replacementByteSequenceCarry,
      chunk,
    ]);
    let count = 0;

    for (let index = 0; index <= searchable.length - 3; index += 1) {
      if (
        searchable[index] === 0xef &&
        searchable[index + 1] === 0xbf &&
        searchable[index + 2] === 0xbd
      ) {
        count += 1;
      }
    }

    this.replacementByteSequenceCarry = searchable.subarray(
      Math.max(0, searchable.length - 2),
    );

    return count;
  }
}
