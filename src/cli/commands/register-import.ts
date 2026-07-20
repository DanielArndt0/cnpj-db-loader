import type { Command } from "commander";

import { confirm } from "../../core/prompts/confirm.js";
import type { ImportOptions } from "../../services/index.js";
import {
  importDataToDatabase,
  loadImportDataToStaging,
  materializeImportedData,
  writeCommandLog,
} from "../../services/index.js";
import {
  createImportProgressReporter,
  printImportSummary,
} from "../ui/output.js";

type SharedOptions = {
  dbUrl?: string;
  dataset?: string;
  loadBatchSize?: number;
  materializeBatchSize?: number;
  verboseProgress?: boolean;
  force?: boolean;
};

function applySharedImportOptions(
  options: SharedOptions,
  config: ImportOptions,
): ImportOptions {
  const nextOptions: ImportOptions = {
    ...config,
  };

  if (options.dbUrl) {
    nextOptions.dbUrl = options.dbUrl;
  }

  if (options.dataset) {
    nextOptions.dataset = options.dataset as ImportOptions["dataset"];
  }

  if (
    typeof options.loadBatchSize === "number" &&
    !Number.isNaN(options.loadBatchSize)
  ) {
    nextOptions.loadBatchSize = options.loadBatchSize;
    nextOptions.batchSize = options.loadBatchSize;
  }

  if (
    typeof options.materializeBatchSize === "number" &&
    !Number.isNaN(options.materializeBatchSize)
  ) {
    nextOptions.materializeBatchSize = options.materializeBatchSize;
  }

  if (options.verboseProgress) {
    nextOptions.verboseProgress = true;
  }

  return nextOptions;
}

function hasForceFlag(
  argv: readonly string[] = process.argv.slice(2),
): boolean {
  return argv.includes("-f") || argv.includes("--force");
}

async function confirmImportAction(
  message: string,
  force?: boolean,
): Promise<boolean> {
  if (force || hasForceFlag()) {
    return true;
  }

  return confirm(message);
}

function registerSharedOptions(command: Command): Command {
  return command
    .option("--db-url <url>", "Sobrescreve a URL de conexão PostgreSQL padrão.")
    .option(
      "--dataset <dataset>",
      "Processa apenas um bloco de dataset validado (por exemplo: companies ou cnaes).",
    )
    .option(
      "--load-batch-size <size>",
      "Número máximo de linhas de origem por unidade de carga de staging. Padrão: 500.",
      (value) => Number.parseInt(value, 10),
    )
    .option(
      "--materialize-batch-size <size>",
      "Número máximo de linhas de staging por bloco de materialização. Padrão: 50000.",
      (value) => Number.parseInt(value, 10),
    )
    .option(
      "--verbose-progress",
      "Mostra o deslocamento de checkpoint e detalhes de lote no progresso ao vivo.",
    )
    .option("-f, --force", "Pula a confirmação interativa.");
}

export function registerImportCommands(program: Command): void {
  const importCommand = registerSharedOptions(
    program
      .command("import")
      .argument("<input>", "Caminho do diretório de entrada extraído ou misto.")
      .description(
        "Executa o pipeline completo de importação: prepara, carrega nos alvos de staging/final, materializa os datasets de staging e finaliza o plano de importação.",
      ),
  );

  importCommand.action(async (input: string, options: SharedOptions) => {
    const confirmed = await confirmImportAction(
      `Executar o pipeline completo de importação para ${input}? Este comando carrega as tabelas de staging e materializa as tabelas relacionais finais.`,
      options.force,
    );
    if (!confirmed) {
      console.log("Importação cancelada.");
      return;
    }

    const progress = createImportProgressReporter();
    const importOptions = applySharedImportOptions(options, {
      onProgress: progress,
    });
    const summary = await importDataToDatabase(input, importOptions);
    const logFilePath = await writeCommandLog("import", summary);
    printImportSummary(summary, logFilePath);
  });

  registerSharedOptions(
    importCommand
      .command("load")
      .argument("<input>", "Caminho do diretório de entrada extraído ou misto.")
      .description(
        "Prepara o plano de importação e carrega os arquivos validados nos alvos de staging ou finais diretos, sem executar a materialização final.",
      ),
  ).action(async (input: string, options: SharedOptions) => {
    const confirmed = await confirmImportAction(
      `Carregar agora os datasets sanitizados de ${input} nos alvos de staging/final? Isto não executa a materialização final.`,
      options.force,
    );
    if (!confirmed) {
      console.log("Carga cancelada.");
      return;
    }

    const progress = createImportProgressReporter();
    const importOptions = applySharedImportOptions(options, {
      onProgress: progress,
    });
    const summary = await loadImportDataToStaging(input, importOptions);
    const logFilePath = await writeCommandLog("import-load", summary);
    printImportSummary(summary, logFilePath);
  });

  registerSharedOptions(
    importCommand
      .command("materialize")
      .argument("<input>", "Caminho do diretório de entrada extraído ou misto.")
      .description(
        "Retoma o plano de importação salvo e materializa os datasets de staging nas tabelas relacionais finais.",
      ),
  ).action(async (input: string, options: SharedOptions) => {
    const confirmed = await confirmImportAction(
      `Materializar agora os datasets de staging de ${input} nas tabelas relacionais finais?`,
      options.force,
    );
    if (!confirmed) {
      console.log("Materialização cancelada.");
      return;
    }

    const progress = createImportProgressReporter();
    const importOptions = applySharedImportOptions(options, {
      onProgress: progress,
    });
    const summary = await materializeImportedData(input, importOptions);
    const logFilePath = await writeCommandLog("import-materialize", summary);
    printImportSummary(summary, logFilePath);
  });
}
