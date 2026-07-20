import { theme } from "../ui/theme.js";

export function rootFooter(): string {
  return `
${theme.section("Fluxo recomendado")}
  ${theme.command("cnpj-db-loader rfb check")}
  ${theme.command("cnpj-db-loader rfb download --output ./downloads --force")}
  ${theme.command("cnpj-db-loader inspect ./downloads/<referencia>")}
  ${theme.command("cnpj-db-loader extract ./downloads/<referencia>")}
  ${theme.command("cnpj-db-loader validate ./downloads/<referencia>/extracted")}
  ${theme.command("cnpj-db-loader sanitize ./downloads/<referencia>/extracted")}
  ${theme.command("cnpj-db-loader schema generate")}
  ${theme.command('cnpj-db-loader database config set "postgresql://user:password@localhost:5432/cnpj"')}
  ${theme.command("cnpj-db-loader database config test")}
  ${theme.command("cnpj-db-loader database cleanup staging --force")}
  ${theme.command("cnpj-db-loader import ./downloads/<referencia>/sanitized")}

${theme.section("Caminho híbrido PostgreSQL")}
  ${theme.command("cnpj-db-loader postgres generate-script ./downloads/<referencia>/sanitized --output ./downloads/<referencia>/postgres-direct --force")}
  ${theme.command('psql "postgres://user:password@localhost:5432/cnpj" -f ./downloads/<referencia>/postgres-direct/import-postgres-direct.sql')}

${theme.section("Notas")}
  ${theme.muted("Use rfb quando quiser que a CLI verifique/baixe primeiro o conjunto mensal remoto. Use inspect primeiro quando já tiver os arquivos locais.")}
  ${theme.muted("Gere o schema apenas quando precisar criar a estrutura do banco. A sanitização é o passo de preparação recomendado antes de importações recorrentes.")}
  ${theme.muted("Use postgres generate-script quando quiser que o PostgreSQL execute a carga pesada diretamente a partir dos arquivos sanitizados da Receita por meio de um script psql gerado.")}
  ${theme.muted("Os logs de execução em JSON são gravados no diretório home do usuário em ~/.cnpjdbloader/logs com metadados estruturados de level/event.")}
`;
}
