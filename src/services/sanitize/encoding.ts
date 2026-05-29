import { StringDecoder } from "node:string_decoder";

import { ValidationError } from "../../core/errors/index.js";

export type SanitizeSourceEncoding = "WIN1252" | "LATIN1" | "UTF8";

export type SanitizedTextChunk = {
  text: string;
  nulBytesRemoved: number;
  invalidBytesRemoved: number;
  controlCharsRemoved: number;
};

const WINDOWS_1252_C1_MAP: Record<number, string | undefined> = {
  0x80: "€",
  0x82: "‚",
  0x83: "ƒ",
  0x84: "„",
  0x85: "…",
  0x86: "†",
  0x87: "‡",
  0x88: "ˆ",
  0x89: "‰",
  0x8a: "Š",
  0x8b: "‹",
  0x8c: "Œ",
  0x8e: "Ž",
  0x91: "‘",
  0x92: "’",
  0x93: "“",
  0x94: "”",
  0x95: "•",
  0x96: "–",
  0x97: "—",
  0x98: "˜",
  0x99: "™",
  0x9a: "š",
  0x9b: "›",
  0x9c: "œ",
  0x9e: "ž",
  0x9f: "Ÿ",
};

export function normalizeSanitizeSourceEncoding(
  value: string | undefined,
): SanitizeSourceEncoding {
  const normalized = (value ?? "WIN1252")
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

function sanitizeDecodedText(
  text: string,
): Omit<SanitizedTextChunk, "nulBytesRemoved"> {
  const output: string[] = [];
  let invalidBytesRemoved = 0;
  let controlCharsRemoved = 0;

  for (const char of text) {
    const codePoint = char.codePointAt(0)!;

    if (codePoint === 0xfffd) {
      invalidBytesRemoved += 1;
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
    invalidBytesRemoved,
    controlCharsRemoved,
  };
}

export class SanitizeEncodingNormalizer {
  private readonly utf8Decoder: StringDecoder | undefined;

  constructor(private readonly sourceEncoding: SanitizeSourceEncoding) {
    this.utf8Decoder =
      sourceEncoding === "UTF8" ? new StringDecoder("utf8") : undefined;
  }

  normalizeChunk(chunk: Buffer): SanitizedTextChunk {
    if (this.sourceEncoding === "UTF8") {
      const decoded = this.utf8Decoder!.write(chunk);
      const sanitized = sanitizeDecodedText(decoded);
      const nulBytesRemoved = [...decoded].filter(
        (char) => char === "\0",
      ).length;

      return {
        ...sanitized,
        nulBytesRemoved,
      };
    }

    return this.normalizeSingleByteChunk(chunk);
  }

  flush(): SanitizedTextChunk {
    if (!this.utf8Decoder) {
      return {
        text: "",
        nulBytesRemoved: 0,
        invalidBytesRemoved: 0,
        controlCharsRemoved: 0,
      };
    }

    const decoded = this.utf8Decoder.end();
    const sanitized = sanitizeDecodedText(decoded);
    const nulBytesRemoved = [...decoded].filter((char) => char === "\0").length;

    return {
      ...sanitized,
      nulBytesRemoved,
    };
  }

  private normalizeSingleByteChunk(chunk: Buffer): SanitizedTextChunk {
    const output: string[] = [];
    let nulBytesRemoved = 0;
    let invalidBytesRemoved = 0;
    let controlCharsRemoved = 0;

    for (const byte of chunk) {
      if (byte === 0x00) {
        nulBytesRemoved += 1;
        continue;
      }

      if (byte < 0x20 || byte === 0x7f) {
        if (isAllowedControlCodePoint(byte)) {
          output.push(String.fromCharCode(byte));
        } else {
          controlCharsRemoved += 1;
        }
        continue;
      }

      if (byte >= 0x80 && byte <= 0x9f) {
        if (this.sourceEncoding === "WIN1252") {
          const mapped = WINDOWS_1252_C1_MAP[byte];
          if (mapped === undefined) {
            invalidBytesRemoved += 1;
          } else {
            output.push(mapped);
          }
        } else {
          controlCharsRemoved += 1;
        }
        continue;
      }

      output.push(String.fromCharCode(byte));
    }

    return {
      text: output.join(""),
      nulBytesRemoved,
      invalidBytesRemoved,
      controlCharsRemoved,
    };
  }
}
