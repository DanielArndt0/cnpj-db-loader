import { Client } from "pg";

import { TABELA_QUARENTENA_IMPORTACAO } from "../schema/table-names.js";
import { ensureTableShape } from "./schema-validation.js";
import type { ImportDatasetType } from "./types.js";

function classifyQuarantineError(error: unknown): {
  code: string;
  category: string;
  canRetryLater: boolean;
} {
  const errorCode =
    typeof error === "object" && error && "code" in error
      ? String((error as { code?: string }).code ?? "QUARANTINED_ROW")
      : "QUARANTINED_ROW";
  const errorMessage =
    error instanceof Error
      ? error.message
      : String(error ?? "Erro desconhecido");
  const normalizedMessage = errorMessage.toLowerCase();

  if (normalizedMessage.includes("invalid byte sequence for encoding")) {
    return {
      code: errorCode,
      category: "invalid_utf8_sequence",
      canRetryLater: true,
    };
  }

  if (normalizedMessage.includes("violates not-null constraint")) {
    return {
      code: errorCode,
      category: "not_null_violation",
      canRetryLater: false,
    };
  }

  if (normalizedMessage.includes("violates foreign key constraint")) {
    return {
      code: errorCode,
      category: "foreign_key_violation",
      canRetryLater: false,
    };
  }

  if (
    normalizedMessage.includes("campos inesperado") ||
    normalizedMessage.includes("field count")
  ) {
    return {
      code: errorCode,
      category: "invalid_field_count",
      canRetryLater: true,
    };
  }

  if (normalizedMessage.includes("transform")) {
    return {
      code: errorCode,
      category: "transform_error",
      canRetryLater: true,
    };
  }

  return {
    code: errorCode,
    category: "unknown",
    canRetryLater: false,
  };
}

export async function ensureQuarantineTable(client: Client): Promise<void> {
  await ensureTableShape(client, {
    tableName: TABELA_QUARENTENA_IMPORTACAO,
    requiredColumns: [
      "conjunto",
      "caminho_arquivo",
      "numero_linha",
      "deslocamento_checkpoint",
      "codigo_erro",
      "categoria_erro",
      "etapa_erro",
      "mensagem_erro",
      "linha_bruta",
      "payload_parseado",
      "sanitizacoes_aplicadas",
      "total_tentativas",
      "pode_tentar_novamente",
      "criado_em",
    ],
    helpMessage:
      'O schema de quarentena de importação é obrigatório. Rode "cnpj-db-loader schema generate --profile full" e aplique o SQL antes de importar.',
  });
}

export type QuarantineInput = {
  dataset: ImportDatasetType;
  filePath: string;
  rowNumber: number;
  checkpointOffset: number;
  rawLine: string;
  error: unknown;
  parsedPayload?: Record<string, unknown> | null;
  errorStage?: string | null;
  sanitizationsApplied?: unknown[] | null;
  retryCount?: number;
  canRetryLater?: boolean;
  errorCategory?: string | null;
};

export async function writeQuarantineRow(
  client: Client,
  input: QuarantineInput,
): Promise<void> {
  const classified = classifyQuarantineError(input.error);
  const errorMessage =
    input.error instanceof Error ? input.error.message : String(input.error);

  await client.query(
    `insert into ${TABELA_QUARENTENA_IMPORTACAO} (
       conjunto,
       caminho_arquivo,
       numero_linha,
       deslocamento_checkpoint,
       codigo_erro,
       categoria_erro,
       etapa_erro,
       mensagem_erro,
       linha_bruta,
       payload_parseado,
       sanitizacoes_aplicadas,
       total_tentativas,
       pode_tentar_novamente,
       criado_em
     ) values (
       $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::jsonb, $12, $13, now()
     )`,
    [
      input.dataset,
      input.filePath,
      input.rowNumber,
      input.checkpointOffset,
      classified.code,
      input.errorCategory ?? classified.category,
      input.errorStage ?? null,
      errorMessage,
      input.rawLine,
      input.parsedPayload ? JSON.stringify(input.parsedPayload) : null,
      input.sanitizationsApplied
        ? JSON.stringify(input.sanitizationsApplied)
        : null,
      input.retryCount ?? 0,
      input.canRetryLater ?? classified.canRetryLater,
    ],
  );
}
