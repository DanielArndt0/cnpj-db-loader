export function formatCsvValue(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }

  if (value instanceof Date) {
    return formatCsvValue(value.toISOString());
  }

  const text = String(value);
  const shouldQuote = /[",\r\n]/.test(text);

  if (!shouldQuote) {
    return text;
  }

  return `"${text.replace(/"/g, '""')}"`;
}

export function formatCsvRow(values: readonly unknown[]): string {
  return values.map(formatCsvValue).join(",");
}
