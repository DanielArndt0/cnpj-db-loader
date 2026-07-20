import type { Command } from "commander";

import { confirm } from "../../core/prompts/confirm.js";
import type { ImportDatasetType } from "../../services/index.js";
import {
  cleanupDatabaseCheckpointsData,
  cleanupDatabaseMaterializedData,
  cleanupDatabasePlansData,
  cleanupDatabaseStagingData,
  readDatabaseConfig,
  resetDefaultDbUrl,
  setDefaultDbUrl,
  testDatabaseConnection,
  writeCommandLog,
} from "../../services/index.js";
import {
  printDatabaseCleanupSummary,
  printDatabaseConfigSummary,
  printInfoWithLog,
} from "../ui/output.js";

type DatabaseGlobalOptions = {
  dbUrl?: string;
  dataset?: string;
  validatedPath?: string;
  planId?: number;
  force?: boolean;
};

async function confirmDatabaseAction(
  message: string,
  force?: boolean,
): Promise<boolean> {
  if (force) {
    return true;
  }

  return confirm(message);
}

function resolveDataset(
  dataset: string | undefined,
): ImportDatasetType | undefined {
  return dataset as ImportDatasetType | undefined;
}

export function registerDatabaseCommands(program: Command): void {
  const database = program
    .command("database")
    .alias("db")
    .description(
      "Gerencia as configurações de conexão PostgreSQL e operações seguras de manutenção.",
    );

  const config = database
    .command("config")
    .description(
      "Lê, persiste, testa ou redefine a conexão PostgreSQL padrão.",
    );

  config
    .command("set")
    .argument("<url>", "String de conexão PostgreSQL a persistir como padrão.")
    .description(
      "Persiste a string de conexão PostgreSQL padrão para os próximos comandos.",
    )
    .action(async (url: string) => {
      await setDefaultDbUrl(url);
      const logFilePath = await writeCommandLog("database-config-set", {
        defaultDbUrl: url,
      });
      printInfoWithLog("DATABASE", "URL padrão do banco salva.", logFilePath);
    });

  config
    .command("show")
    .description("Mostra a configuração de banco atualmente persistida.")
    .action(async () => {
      const currentConfig = await readDatabaseConfig();
      const logFilePath = await writeCommandLog(
        "database-config-show",
        currentConfig,
      );
      printDatabaseConfigSummary(currentConfig, logFilePath);
    });

  config
    .command("test")
    .option("--db-url <url>", "Sobrescreve a URL de conexão PostgreSQL padrão.")
    .description("Testa a conexão PostgreSQL.")
    .action(async (options: { dbUrl?: string }) => {
      const currentConfig = await readDatabaseConfig();
      const url = options.dbUrl ?? currentConfig.defaultDbUrl;

      if (!url) {
        console.log("Nenhuma URL de banco disponível para o teste de conexão.");
        process.exitCode = 1;
        return;
      }

      await testDatabaseConnection(url);
      const logFilePath = await writeCommandLog("database-config-test", {
        testedUrl: url,
        ok: true,
      });
      printInfoWithLog(
        "DATABASE",
        "Conexão com o banco bem-sucedida.",
        logFilePath,
      );
    });

  config
    .command("reset")
    .option("-f, --force", "Pula a confirmação interativa.")
    .description("Remove a conexão de banco padrão persistida.")
    .action(async (options: { force?: boolean }) => {
      const confirmed = await confirmDatabaseAction(
        "Remover a configuração de banco padrão persistida?",
        options.force,
      );
      if (!confirmed) {
        console.log("Redefinição do banco cancelada.");
        return;
      }

      await resetDefaultDbUrl();
      const logFilePath = await writeCommandLog("database-config-reset", {
        ok: true,
      });
      printInfoWithLog(
        "DATABASE",
        "URL padrão do banco removida.",
        logFilePath,
      );
    });

  const cleanup = database
    .command("cleanup")
    .description(
      "Limpa com segurança dados de staging, tabelas finais materializadas, checkpoints ou planos de importação salvos.",
    );

  cleanup
    .command("staging")
    .option("--db-url <url>", "Sobrescreve a URL de conexão PostgreSQL padrão.")
    .option(
      "--dataset <dataset>",
      "Restringe a limpeza a um dataset de staging (companies, establishments, partners, simples_options).",
    )
    .option(
      "--validated-path <path>",
      "Também limpa os checkpoints de materialização vinculados ao(s) último(s) plano(s) salvo(s) deste caminho validado.",
    )
    .option("-f, --force", "Pula a confirmação interativa.")
    .description(
      "Trunca as tabelas de staging para que uma nova carga em massa reinicie a partir de um estado intermediário limpo.",
    )
    .action(async (options: DatabaseGlobalOptions) => {
      const confirmed = await confirmDatabaseAction(
        "Truncar agora as tabelas de staging? Isto remove os dados intermediários da carga em massa e pode também limpar os checkpoints de materialização quando --validated-path é usado.",
        options.force,
      );
      if (!confirmed) {
        console.log("Limpeza de staging cancelada.");
        return;
      }

      const cleanupOptions: {
        dbUrl?: string;
        dataset?: ImportDatasetType | undefined;
        validatedPath?: string | undefined;
      } = {};
      if (options.dbUrl) {
        cleanupOptions.dbUrl = options.dbUrl;
      }
      if (options.dataset) {
        cleanupOptions.dataset = resolveDataset(options.dataset);
      }
      if (options.validatedPath) {
        cleanupOptions.validatedPath = options.validatedPath;
      }

      const summary = await cleanupDatabaseStagingData(cleanupOptions);
      const logFilePath = await writeCommandLog(
        "database-cleanup-staging",
        summary,
      );
      printDatabaseCleanupSummary(summary, logFilePath);
    });

  cleanup
    .command("materialized")
    .alias("final")
    .option("--db-url <url>", "Sobrescreve a URL de conexão PostgreSQL padrão.")
    .option(
      "--dataset <dataset>",
      "Restringe a limpeza a um dataset final materializado simplificado (companies, establishments, partners, simples_options).",
    )
    .option("-f, --force", "Pula a confirmação interativa.")
    .description(
      "Trunca as tabelas relacionais finais simplificadas populadas pela materialização, em ordem segura para o schema atual.",
    )
    .action(async (options: DatabaseGlobalOptions) => {
      const confirmed = await confirmDatabaseAction(
        "Truncar agora as tabelas finais materializadas simplificadas? Isto remove os dados relacionais já consolidados a partir do staging.",
        options.force,
      );
      if (!confirmed) {
        console.log("Limpeza das tabelas materializadas cancelada.");
        return;
      }

      const cleanupOptions: {
        dbUrl?: string;
        dataset?: ImportDatasetType | undefined;
      } = {};
      if (options.dbUrl) {
        cleanupOptions.dbUrl = options.dbUrl;
      }
      if (options.dataset) {
        cleanupOptions.dataset = resolveDataset(options.dataset);
      }

      const summary = await cleanupDatabaseMaterializedData(cleanupOptions);
      const logFilePath = await writeCommandLog(
        "database-cleanup-materialized",
        summary,
      );
      printDatabaseCleanupSummary(summary, logFilePath);
    });

  cleanup
    .command("checkpoints")
    .option("--db-url <url>", "Sobrescreve a URL de conexão PostgreSQL padrão.")
    .option(
      "--phase <phase>",
      "Escolhe qual família de checkpoints limpar: load, materialization ou all. Padrão: all.",
    )
    .option(
      "--dataset <dataset>",
      "Restringe a limpeza a um dataset. Checkpoints de carga aceitam qualquer dataset de importação; checkpoints de materialização aceitam apenas datasets de staging.",
    )
    .option(
      "--validated-path <path>",
      "Limita a limpeza de checkpoints de materialização ao(s) plano(s) associado(s) a este caminho validado.",
    )
    .option(
      "--plan-id <id>",
      "Limita a limpeza de checkpoints de materialização a um id de plano de importação específico.",
      (value) => Number.parseInt(value, 10),
    )
    .option("-f, --force", "Pula a confirmação interativa.")
    .description(
      "Limpa checkpoints de carga, de materialização, ou ambos, sem truncar as tabelas de dados.",
    )
    .action(
      async (
        options: DatabaseGlobalOptions & {
          phase?: "load" | "materialization" | "all";
        },
      ) => {
        const phase = options.phase ?? "all";
        const confirmed = await confirmDatabaseAction(
          `Limpar agora os dados de checkpoint da fase ${phase}? Isto afeta o estado de retomada salvo, mas não trunca as tabelas de staging ou finais.`,
          options.force,
        );
        if (!confirmed) {
          console.log("Limpeza de checkpoints cancelada.");
          return;
        }

        const cleanupOptions: {
          dbUrl?: string;
          phase?: "load" | "materialization" | "all";
          dataset?: ImportDatasetType | undefined;
          validatedPath?: string | undefined;
          planId?: number | undefined;
        } = { phase };
        if (options.dbUrl) {
          cleanupOptions.dbUrl = options.dbUrl;
        }
        if (options.dataset) {
          cleanupOptions.dataset = resolveDataset(options.dataset);
        }
        if (options.validatedPath) {
          cleanupOptions.validatedPath = options.validatedPath;
        }
        if (
          typeof options.planId === "number" &&
          !Number.isNaN(options.planId)
        ) {
          cleanupOptions.planId = options.planId;
        }

        const summary = await cleanupDatabaseCheckpointsData(cleanupOptions);
        const logFilePath = await writeCommandLog(
          "database-cleanup-checkpoints",
          summary,
        );
        printDatabaseCleanupSummary(summary, logFilePath);
      },
    );

  cleanup
    .command("plans")
    .option("--db-url <url>", "Sobrescreve a URL de conexão PostgreSQL padrão.")
    .option(
      "--validated-path <path>",
      "Exclui o(s) plano(s) de importação salvo(s) associado(s) a este caminho validado no banco selecionado.",
    )
    .option(
      "--plan-id <id>",
      "Exclui apenas um plano de importação salvo, por id.",
      (value) => Number.parseInt(value, 10),
    )
    .option("-f, --force", "Pula a confirmação interativa.")
    .description(
      "Exclui os planos de importação salvos. Os arquivos de plano e os checkpoints de materialização relacionados são removidos por cascata do banco.",
    )
    .action(async (options: DatabaseGlobalOptions) => {
      const confirmed = await confirmDatabaseAction(
        "Excluir agora os planos de importação salvos? Isto remove os metadados de orquestração e os checkpoints de materialização vinculados.",
        options.force,
      );
      if (!confirmed) {
        console.log("Limpeza de planos cancelada.");
        return;
      }

      const cleanupOptions: {
        dbUrl?: string;
        validatedPath?: string | undefined;
        planId?: number | undefined;
      } = {};
      if (options.dbUrl) {
        cleanupOptions.dbUrl = options.dbUrl;
      }
      if (options.validatedPath) {
        cleanupOptions.validatedPath = options.validatedPath;
      }
      if (typeof options.planId === "number" && !Number.isNaN(options.planId)) {
        cleanupOptions.planId = options.planId;
      }

      const summary = await cleanupDatabasePlansData(cleanupOptions);
      const logFilePath = await writeCommandLog(
        "database-cleanup-plans",
        summary,
      );
      printDatabaseCleanupSummary(summary, logFilePath);
    });
}
