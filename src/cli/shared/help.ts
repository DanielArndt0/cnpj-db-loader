import { theme } from "../ui/theme.js";

export function rootFooter(): string {
  return `
${theme.section("Recommended flow")}
  ${theme.command("cnpj-db-loader inspect ./downloads")}
  ${theme.command("cnpj-db-loader extract ./downloads")}
  ${theme.command("cnpj-db-loader validate ./downloads/extracted")}
  ${theme.command("cnpj-db-loader sanitize ./downloads/extracted")}
  ${theme.command("cnpj-db-loader schema generate")}
  ${theme.command('cnpj-db-loader db set "postgresql://user:password@localhost:5432/cnpj"')}
  ${theme.command("cnpj-db-loader db test")}
  ${theme.command("cnpj-db-loader import ./downloads/sanitized")}

${theme.section("Notes")}
  ${theme.muted("Use inspect first to understand whether the input is still zipped or already extracted.")}
  ${theme.muted("Generate the schema only when you need to create the database structure. Sanitization is the recommended preparation step before recurring imports.")}
  ${theme.muted("JSON execution logs are written to ./logs in the current working directory.")}
`;
}
