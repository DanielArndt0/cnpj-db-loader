export type SanitizationResult = {
  value: string;
  changed: boolean;
  actions: string[];
};

export function sanitizeRawLine(value: string): SanitizationResult {
  let sanitized = value;
  const actions: string[] = [];

  if (sanitized.includes("\u0000")) {
    sanitized = sanitized.replaceAll("\u0000", "");
    actions.push("remove_nul_bytes");
  }

  return {
    value: sanitized,
    changed: actions.length > 0,
    actions,
  };
}

export function sanitizeText(value: string): string {
  return value.includes("\u0000") ? value.replaceAll("\u0000", "") : value;
}

export function deepSanitizeForJson<T>(input: T): T {
  if (typeof input === "string") {
    return sanitizeText(input) as T;
  }

  if (Array.isArray(input)) {
    return input.map((item) => deepSanitizeForJson(item)) as T;
  }

  if (input && typeof input === "object") {
    const output: Record<string, unknown> = {};

    for (const [key, value] of Object.entries(
      input as Record<string, unknown>,
    )) {
      output[key] = deepSanitizeForJson(value);
    }

    return output as T;
  }

  return input;
}
