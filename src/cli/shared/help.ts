import { theme } from "../ui/theme.js";

export function rootFooter(): string {
  return `
${theme.section("Recommended flow")}
  ${theme.command("cnpj-db-loader federal-revenue check")}
  ${theme.command("cnpj-db-loader federal-revenue download --output ./downloads --force")}
  ${theme.command("cnpj-db-loader inspect ./downloads/<reference>")}
  ${theme.command("cnpj-db-loader extract ./downloads/<reference>")}
  ${theme.command("cnpj-db-loader validate ./downloads/<reference>/extracted")}
  ${theme.command("cnpj-db-loader sanitize ./downloads/<reference>/extracted")}
  ${theme.command("cnpj-db-loader schema generate")}
  ${theme.command('cnpj-db-loader database config set "postgresql://user:password@localhost:5432/cnpj"')}
  ${theme.command("cnpj-db-loader database config test")}
  ${theme.command("cnpj-db-loader database cleanup staging --force")}
  ${theme.command("cnpj-db-loader import ./downloads/<reference>/sanitized")}

${theme.section("Notes")}
  ${theme.muted("Use federal-revenue when you want the CLI to check/download the remote monthly dataset first. Use inspect first when you already have local files.")}
  ${theme.muted("Generate the schema only when you need to create the database structure. Sanitization is the recommended preparation step before recurring imports.")}
  ${theme.muted("JSON execution logs are written inside the user home directory at ~/.cnpjdbloader/logs with structured level/event metadata.")}
`;
}
