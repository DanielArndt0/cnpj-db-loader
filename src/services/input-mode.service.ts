export type InputMode = "unzip" | "already-extracted";

export function resolveInputMode(options: {
  unzip?: boolean;
  alreadyExtracted?: boolean;
}): InputMode {
  if (options.unzip && options.alreadyExtracted) {
    throw new Error(
      'Escolha apenas um modo de entrada: use "--unzip" ou "--already-extracted".',
    );
  }

  if (options.unzip) {
    return "unzip";
  }

  return "already-extracted";
}
