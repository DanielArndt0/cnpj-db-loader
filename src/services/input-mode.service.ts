export type InputMode = "unzip" | "already-extracted";

export function resolveInputMode(options: {
  unzip?: boolean;
  alreadyExtracted?: boolean;
}): InputMode {
  if (options.unzip && options.alreadyExtracted) {
    throw new Error(
      'Choose only one input mode: use either "--unzip" or "--already-extracted".',
    );
  }

  if (options.unzip) {
    return "unzip";
  }

  return "already-extracted";
}
