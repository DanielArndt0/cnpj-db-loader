import type { Command } from "commander";

import { ValidationError } from "../../core/errors/index.js";
import { confirm } from "../../core/prompts/confirm.js";
import type {
  FederalRevenueCheckOptions,
  FederalRevenueCleanOptions,
  FederalRevenueDownloadOptions,
  FederalRevenueStatusOptions,
  FederalRevenueSyncOptions,
  ImportOptions,
} from "../../services/index.js";
import {
  checkFederalRevenueDataset,
  cleanFederalRevenueDataset,
  downloadFederalRevenueDataset,
  getFederalRevenueStatus,
  listFederalRevenueReferences,
  readFederalRevenueEffectiveConfig,
  resetFederalRevenueConfig,
  retryFederalRevenueDataset,
  resolveFederalRevenueClientOptions,
  setFederalRevenueConfigValue,
  syncFederalRevenueDataset,
  writeCommandLog,
} from "../../services/index.js";
import {
  createExtractionProgressReporter,
  createFederalRevenueDownloadProgressReporter,
  createImportProgressReporter,
  createSanitizeProgressReporter,
  printFederalRevenueCheckSummary,
  printFederalRevenueCleanSummary,
  printFederalRevenueConfigSummary,
  printFederalRevenueDownloadSummary,
  printFederalRevenueStatusSummary,
  printFederalRevenueSyncSummary,
} from "../ui/output.js";

type FederalRevenueSharedOptions = {
  reference?: string;
  current?: boolean;
  baseUrl?: string;
  shareToken?: string;
  userAgent?: string;
};

type FederalRevenueDownloadCommandOptions = FederalRevenueSharedOptions & {
  output?: string;
  retries?: number;
  overwrite?: boolean;
  force?: boolean;
};

type FederalRevenueStatusCommandOptions = FederalRevenueSharedOptions & {
  output?: string;
};

type FederalRevenueRetryCommandOptions = FederalRevenueDownloadCommandOptions;

type FederalRevenueCleanCommandOptions = FederalRevenueSharedOptions & {
  output?: string;
  partials?: boolean;
  failed?: boolean;
  all?: boolean;
  force?: boolean;
};

type FederalRevenueSyncCommandOptions = FederalRevenueDownloadCommandOptions & {
  extractOutput?: string;
  sanitizeOutput?: string;
  dbUrl?: string;
  dataset?: string;
  loadBatchSize?: number;
  materializeBatchSize?: number;
  verboseProgress?: boolean;
  forceLock?: boolean;
};

function mergeSharedOptions(
  referenceArgument: string | undefined,
  options: FederalRevenueSharedOptions,
): FederalRevenueSharedOptions {
  if (
    referenceArgument &&
    options.reference &&
    referenceArgument !== options.reference
  ) {
    throw new ValidationError(
      `Conflito de referência da Receita Federal: recebido ${referenceArgument} e ${options.reference}. Use apenas um valor de referência.`,
    );
  }

  const reference = options.reference ?? referenceArgument;

  if (reference && options.current) {
    throw new ValidationError(
      "Conflito de referência da Receita Federal: use uma referência ou --current, não ambos.",
    );
  }

  return {
    ...options,
    ...(reference ? { reference } : {}),
  };
}

function applySharedOptions<T extends FederalRevenueCheckOptions>(
  options: FederalRevenueSharedOptions,
  target: T,
): T {
  if (options.reference) {
    target.reference = options.reference;
  }

  if (options.current) {
    target.current = true;
  }

  if (options.baseUrl) {
    target.baseUrl = options.baseUrl;
  }

  if (options.shareToken) {
    target.shareToken = options.shareToken;
  }

  if (options.userAgent) {
    target.userAgent = options.userAgent;
  }

  return target;
}

async function resolveSharedOptions(
  referenceArgument: string | undefined,
  options: FederalRevenueSharedOptions,
): Promise<FederalRevenueSharedOptions> {
  const mergedOptions = mergeSharedOptions(referenceArgument, options);
  const clientOptions = await resolveFederalRevenueClientOptions(mergedOptions);

  return {
    ...mergedOptions,
    ...(clientOptions.baseUrl ? { baseUrl: clientOptions.baseUrl } : {}),
    ...(clientOptions.shareToken
      ? { shareToken: clientOptions.shareToken }
      : {}),
    ...(clientOptions.userAgent ? { userAgent: clientOptions.userAgent } : {}),
  };
}

function buildDownloadOptions(
  options: FederalRevenueDownloadCommandOptions,
): FederalRevenueDownloadOptions {
  const downloadOptions = applySharedOptions<FederalRevenueDownloadOptions>(
    options,
    {},
  );

  if (options.output) {
    downloadOptions.outputPath = options.output;
  }

  if (typeof options.retries === "number" && !Number.isNaN(options.retries)) {
    downloadOptions.retries = options.retries;
  }

  if (options.overwrite) {
    downloadOptions.overwrite = true;
  }

  return downloadOptions;
}

function buildStatusOptions(
  options: FederalRevenueStatusCommandOptions,
): FederalRevenueStatusOptions {
  const statusOptions = applySharedOptions<FederalRevenueStatusOptions>(
    options,
    {},
  );

  if (options.output) {
    statusOptions.outputPath = options.output;
  }

  return statusOptions;
}

function buildCleanOptions(
  options: FederalRevenueCleanCommandOptions,
): FederalRevenueCleanOptions {
  const cleanOptions = applySharedOptions<FederalRevenueCleanOptions>(
    options,
    {},
  );

  if (options.output) {
    cleanOptions.outputPath = options.output;
  }

  if (options.partials) {
    cleanOptions.partials = true;
  }

  if (options.failed) {
    cleanOptions.failed = true;
  }

  if (options.all) {
    cleanOptions.all = true;
  }

  return cleanOptions;
}

function buildImportOptions(
  options: FederalRevenueSyncCommandOptions,
): Omit<ImportOptions, "onProgress"> {
  const importOptions: Omit<ImportOptions, "onProgress"> = {};

  if (options.dbUrl) {
    importOptions.dbUrl = options.dbUrl;
  }

  if (options.dataset) {
    importOptions.dataset = options.dataset as ImportOptions["dataset"];
  }

  if (
    typeof options.loadBatchSize === "number" &&
    !Number.isNaN(options.loadBatchSize)
  ) {
    importOptions.loadBatchSize = options.loadBatchSize;
    importOptions.batchSize = options.loadBatchSize;
  }

  if (
    typeof options.materializeBatchSize === "number" &&
    !Number.isNaN(options.materializeBatchSize)
  ) {
    importOptions.materializeBatchSize = options.materializeBatchSize;
  }

  if (options.verboseProgress) {
    importOptions.verboseProgress = true;
  }

  return importOptions;
}

async function confirmFederalRevenueAction(
  message: string,
  force?: boolean,
): Promise<boolean> {
  if (force) {
    return true;
  }

  return confirm(message);
}

function registerSharedOptions(command: Command): Command {
  return command
    .option(
      "--reference <yyyy-mm>",
      "Usa uma referência mensal explícita da Receita Federal, por exemplo 2026-05.",
    )
    .option(
      "--current",
      "Usa o mês calendário atual em vez da última referência publicada.",
    )
    .option(
      "--base-url <url>",
      "Sobrescreve a URL base do WebDAV público da Receita Federal.",
    )
    .option(
      "--share-token <token>",
      "Sobrescreve o token do compartilhamento público da Receita Federal.",
    )
    .option(
      "--user-agent <value>",
      "Sobrescreve o user agent HTTP usado com a Receita Federal.",
    );
}

function registerDownloadOptions(command: Command): Command {
  return registerSharedOptions(command)
    .option(
      "--output <path>",
      "Diretório raiz de download. Uma subpasta com o nome da referência selecionada é criada dentro dele.",
    )
    .option(
      "--retries <number>",
      "Tentativas de repetição por arquivo antes de marcá-lo como falho. Padrão: 3.",
      (value) => Number.parseInt(value, 10),
    )
    .option(
      "--overwrite",
      "Baixa os arquivos novamente mesmo quando já existe uma cópia local completa.",
    )
    .option("-f, --force", "Pula a confirmação interativa.");
}

function registerStatusOptions(command: Command): Command {
  return registerSharedOptions(command).option(
    "--output <path>",
    "Diretório raiz de download onde a pasta da referência selecionada está armazenada.",
  );
}

function registerCleanOptions(command: Command): Command {
  return registerStatusOptions(command)
    .option(
      "--partials",
      "Remove apenas os arquivos .part da referência selecionada.",
    )
    .option(
      "--failed",
      "Remove os arquivos falhos e parciais registrados no manifesto local.",
    )
    .option(
      "--all",
      "Remove toda a pasta local da referência, incluindo arquivos ZIP e o estado do manifesto.",
    )
    .option("-f, --force", "Pula a confirmação interativa.");
}

export function registerFederalRevenueCommands(program: Command): void {
  const federalRevenue = program
    .command("rfb")
    .aliases(["federal-revenue", "revenue"])
    .description(
      "Verifica, baixa, sincroniza e mantém os arquivos mensais de CNPJ do compartilhamento público da Receita Federal.",
    );

  federalRevenue.hook("preAction", () => {
    if (process.argv[2] === "federal-revenue") {
      process.stderr.write(
        'O comando "federal-revenue" foi renomeado para "rfb". Use "cnpj-db-loader rfb ..." nas próximas versões.\n',
      );
    }
  });

  const config = federalRevenue
    .command("config")
    .description(
      "Lê, persiste, testa ou redefine as configurações do compartilhamento público da Receita Federal.",
    );

  config
    .command("set")
    .argument(
      "<key>",
      "Chave de configuração: share-token, webdav-url ou user-agent.",
    )
    .argument("<value>", "Valor de configuração a persistir.")
    .description(
      "Persiste uma configuração da Receita Federal no arquivo de config local do CNPJ DB Loader.",
    )
    .action(async (key: string, value: string) => {
      const effectiveConfig = await setFederalRevenueConfigValue(key, value);
      const logFilePath = await writeCommandLog("federal-revenue-config-set", {
        key,
        effectiveConfig,
      });
      printFederalRevenueConfigSummary(effectiveConfig, logFilePath);
    });

  config
    .command("show")
    .description(
      "Mostra a configuração da Receita Federal atualmente persistida.",
    )
    .action(async () => {
      const effectiveConfig = await readFederalRevenueEffectiveConfig();
      const logFilePath = await writeCommandLog(
        "federal-revenue-config-show",
        effectiveConfig,
      );
      printFederalRevenueConfigSummary(effectiveConfig, logFilePath);
    });

  config
    .command("test")
    .description("Testa a conexão WebDAV configurada da Receita Federal.")
    .action(async () => {
      const clientOptions = await resolveFederalRevenueClientOptions();
      const result = await listFederalRevenueReferences(clientOptions);
      const references = result.references.map((item) => item.reference);
      const latestReference = references.at(-1) ?? "não encontrada";
      const logFilePath = await writeCommandLog("federal-revenue-config-test", {
        remoteBaseUrl: result.remoteBaseUrl,
        referencesFound: references.length,
        latestReference,
      });
      printFederalRevenueConfigSummary(
        await readFederalRevenueEffectiveConfig(),
        logFilePath,
      );
      console.log(
        `Conexão WebDAV da Receita Federal bem-sucedida. Referências encontradas: ${references.length}. Última referência: ${latestReference}.`,
      );
    });

  config
    .command("reset")
    .argument(
      "[key]",
      "Chave opcional a redefinir: share-token, webdav-url ou user-agent. Quando omitida, todas as configurações da Receita Federal são redefinidas.",
    )
    .option("-f, --force", "Pula a confirmação interativa.")
    .description(
      "Redefine uma configuração da Receita Federal ou todas as configurações persistidas.",
    )
    .action(async (key: string | undefined, options: { force?: boolean }) => {
      const target = key
        ? `a configuração ${key} da Receita Federal`
        : "todas as configurações da Receita Federal";
      const confirmed = await confirmFederalRevenueAction(
        `Redefinir ${target}?`,
        options.force,
      );
      if (!confirmed) {
        console.log(
          "Redefinição da configuração da Receita Federal cancelada.",
        );
        return;
      }

      const effectiveConfig = await resetFederalRevenueConfig(key);
      const logFilePath = await writeCommandLog(
        "federal-revenue-config-reset",
        { key: key ?? "all", effectiveConfig },
      );
      printFederalRevenueConfigSummary(effectiveConfig, logFilePath);
    });

  registerSharedOptions(
    federalRevenue
      .command("check")
      .argument(
        "[reference]",
        "Referência mensal opcional no formato YYYY-MM. Igual a --reference.",
      )
      .description(
        "Verifica a última referência mensal de CNPJ disponível na Receita Federal e lista os arquivos ZIP.",
      ),
  ).action(
    async (
      referenceArgument: string | undefined,
      options: FederalRevenueSharedOptions,
    ) => {
      const resolvedOptions = await resolveSharedOptions(
        referenceArgument,
        options,
      );
      const summary = await checkFederalRevenueDataset(
        applySharedOptions<FederalRevenueCheckOptions>(resolvedOptions, {}),
      );
      const logFilePath = await writeCommandLog(
        "federal-revenue-check",
        summary,
      );
      printFederalRevenueCheckSummary(summary, logFilePath);
    },
  );

  registerDownloadOptions(
    federalRevenue
      .command("download")
      .argument(
        "[reference]",
        "Referência mensal opcional no formato YYYY-MM. Igual a --reference.",
      )
      .description(
        "Baixa os arquivos ZIP mensais de CNPJ da referência selecionada com arquivos .part seguros, estado de manifesto e repetições.",
      ),
  ).action(
    async (
      referenceArgument: string | undefined,
      options: FederalRevenueDownloadCommandOptions,
    ) => {
      const resolvedOptions = await resolveSharedOptions(
        referenceArgument,
        options,
      );
      const confirmed = await confirmFederalRevenueAction(
        "Baixar agora os arquivos ZIP de CNPJ da Receita Federal? Arquivos já completos são ignorados, a menos que --overwrite seja usado.",
        options.force,
      );
      if (!confirmed) {
        console.log("Download da Receita Federal cancelado.");
        return;
      }

      const progress = createFederalRevenueDownloadProgressReporter();
      const summary = await downloadFederalRevenueDataset({
        ...buildDownloadOptions({ ...options, ...resolvedOptions }),
        onProgress: progress,
      });
      const logFilePath = await writeCommandLog(
        "federal-revenue-download",
        summary,
      );
      printFederalRevenueDownloadSummary(summary, logFilePath);

      if (summary.failedFiles > 0) {
        process.exitCode = 1;
      }
    },
  );

  registerStatusOptions(
    federalRevenue
      .command("status")
      .argument(
        "[reference]",
        "Referência mensal opcional no formato YYYY-MM. Igual a --reference.",
      )
      .description(
        "Lê o manifesto local da Receita Federal e informa arquivos baixados, falhos, parciais e ausentes.",
      ),
  ).action(
    async (
      referenceArgument: string | undefined,
      options: FederalRevenueStatusCommandOptions,
    ) => {
      const resolvedOptions = await resolveSharedOptions(
        referenceArgument,
        options,
      );
      const summary = await getFederalRevenueStatus(
        buildStatusOptions({ ...options, ...resolvedOptions }),
      );
      const logFilePath = await writeCommandLog(
        "federal-revenue-status",
        summary,
      );
      printFederalRevenueStatusSummary(summary, logFilePath);

      if (!summary.isComplete) {
        process.exitCode = 1;
      }
    },
  );

  registerDownloadOptions(
    federalRevenue
      .command("retry")
      .argument(
        "[reference]",
        "Referência mensal opcional no formato YYYY-MM. Igual a --reference.",
      )
      .description(
        "Repete apenas os arquivos incompletos da Receita Federal registrados no manifesto local.",
      ),
  ).action(
    async (
      referenceArgument: string | undefined,
      options: FederalRevenueRetryCommandOptions,
    ) => {
      const resolvedOptions = await resolveSharedOptions(
        referenceArgument,
        options,
      );
      const confirmed = await confirmFederalRevenueAction(
        "Repetir agora os arquivos incompletos da Receita Federal? Arquivos completos são mantidos.",
        options.force,
      );
      if (!confirmed) {
        console.log("Repetição da Receita Federal cancelada.");
        return;
      }

      const progress = createFederalRevenueDownloadProgressReporter();
      const summary = await retryFederalRevenueDataset({
        ...buildDownloadOptions({ ...options, ...resolvedOptions }),
        onProgress: progress,
      });
      const logFilePath = await writeCommandLog(
        "federal-revenue-retry",
        summary,
      );
      printFederalRevenueDownloadSummary(summary, logFilePath);

      if (
        summary.failedFiles > 0 ||
        summary.partialFiles > 0 ||
        summary.missingFiles > 0
      ) {
        process.exitCode = 1;
      }
    },
  );

  registerCleanOptions(
    federalRevenue
      .command("clean")
      .argument(
        "[reference]",
        "Referência mensal opcional no formato YYYY-MM. Igual a --reference.",
      )
      .description(
        "Limpa arquivos parciais, arquivos falhos ou toda a pasta de uma referência local da Receita Federal.",
      ),
  ).action(
    async (
      referenceArgument: string | undefined,
      options: FederalRevenueCleanCommandOptions,
    ) => {
      const resolvedOptions = await resolveSharedOptions(
        referenceArgument,
        options,
      );
      const actionLabel = options.all
        ? "remover toda a pasta da referência selecionada da Receita Federal"
        : options.failed
          ? "remover arquivos falhos e parciais da Receita Federal"
          : "remover arquivos .part da Receita Federal";
      const confirmed = await confirmFederalRevenueAction(
        `Isto irá ${actionLabel}. Continuar?`,
        options.force,
      );
      if (!confirmed) {
        console.log("Limpeza da Receita Federal cancelada.");
        return;
      }

      const summary = await cleanFederalRevenueDataset(
        buildCleanOptions({ ...options, ...resolvedOptions }),
      );
      const logFilePath = await writeCommandLog(
        "federal-revenue-clean",
        summary,
      );
      printFederalRevenueCleanSummary(summary, logFilePath);
    },
  );

  registerDownloadOptions(
    federalRevenue
      .command("sync")
      .argument(
        "[reference]",
        "Referência mensal opcional no formato YYYY-MM. Igual a --reference.",
      )
      .option(
        "--extract-output <path>",
        "Diretório de saída da extração. Padrão: <download-referencia>/extracted.",
      )
      .option(
        "--sanitize-output <path>",
        "Diretório de saída da sanitização. Padrão: <download-referencia>/sanitized.",
      )
      .option(
        "--db-url <url>",
        "Sobrescreve a URL de conexão PostgreSQL padrão.",
      )
      .option(
        "--dataset <dataset>",
        "Processa apenas um bloco de dataset validado durante a importação.",
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
        "Mostra o deslocamento de checkpoint e detalhes de lote no progresso ao vivo da importação.",
      )
      .option(
        "--force-lock",
        "Remove um lock de sync local existente antes de iniciar. Use apenas após confirmar que o processo anterior parou.",
      )
      .description(
        "Baixa, extrai, valida, sanitiza e importa o dataset mensal de CNPJ da referência selecionada da Receita Federal.",
      ),
  ).action(
    async (
      referenceArgument: string | undefined,
      options: FederalRevenueSyncCommandOptions,
    ) => {
      const resolvedOptions = await resolveSharedOptions(
        referenceArgument,
        options,
      );
      const confirmed = await confirmFederalRevenueAction(
        "Executar agora o sync completo da Receita Federal? Isto baixa arquivos, extrai os pacotes, sanitiza o dataset e o importa para o PostgreSQL.",
        options.force,
      );
      if (!confirmed) {
        console.log("Sync da Receita Federal cancelado.");
        return;
      }

      const downloadProgress = createFederalRevenueDownloadProgressReporter();
      const extractProgress = createExtractionProgressReporter();
      const sanitizeProgress = createSanitizeProgressReporter();
      const importProgress = createImportProgressReporter();

      const commandOptions = { ...options, ...resolvedOptions };
      const syncOptions: FederalRevenueSyncOptions = {
        ...buildDownloadOptions(commandOptions),
        onProgress: downloadProgress,
        onExtractProgress: extractProgress,
        onSanitizeProgress: sanitizeProgress,
        onImportProgress: importProgress,
        importOptions: buildImportOptions(commandOptions),
      };

      if (options.extractOutput) {
        syncOptions.extractOutputPath = options.extractOutput;
      }

      if (options.sanitizeOutput) {
        syncOptions.sanitizeOutputPath = options.sanitizeOutput;
      }

      if (options.forceLock) {
        syncOptions.forceLock = true;
      }

      const summary = await syncFederalRevenueDataset(syncOptions);
      const logFilePath = await writeCommandLog(
        "federal-revenue-sync",
        summary,
      );
      printFederalRevenueSyncSummary(summary, logFilePath);
    },
  );
}
