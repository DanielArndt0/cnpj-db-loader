const TWO_PART_COMMANDS = new Set(["quarantine"]);
const THREE_PART_COMMANDS = new Set(["database"]);

export function resolveInvokedCommandLabel(argv: string[]): string {
  const tokens = argv.filter((token) => token.trim() !== "");

  if (tokens.length === 0) {
    return "cli";
  }

  const [firstToken, secondToken, thirdToken] = tokens;

  if (
    firstToken !== undefined &&
    secondToken !== undefined &&
    thirdToken !== undefined &&
    !firstToken.startsWith("-") &&
    !secondToken.startsWith("-") &&
    !thirdToken.startsWith("-") &&
    THREE_PART_COMMANDS.has(firstToken)
  ) {
    return `${firstToken}-${secondToken}-${thirdToken}`;
  }

  if (
    firstToken !== undefined &&
    secondToken !== undefined &&
    !firstToken.startsWith("-") &&
    !secondToken.startsWith("-") &&
    TWO_PART_COMMANDS.has(firstToken)
  ) {
    return `${firstToken}-${secondToken}`;
  }

  if (firstToken !== undefined && !firstToken.startsWith("-")) {
    return firstToken;
  }

  return "cli";
}
