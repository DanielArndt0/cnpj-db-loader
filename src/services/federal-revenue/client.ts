import { ValidationError } from "../../core/errors/index.js";
import type {
  FederalRevenueClientOptions,
  FederalRevenueFile,
  FederalRevenueReference,
  FederalRevenueReferenceSelection,
} from "./types.js";

export const DEFAULT_FEDERAL_REVENUE_WEBDAV_URL =
  "https://arquivos.receitafederal.gov.br/public.php/webdav";
export const DEFAULT_FEDERAL_REVENUE_USER_AGENT =
  "cnpj-db-loader federal-revenue-client";

const REFERENCE_PATTERN = /^\d{4}-\d{2}$/;

function trimTrailingSlash(value: string): string {
  return value.replace(/\/+$/g, "");
}

function normalizeBaseUrl(value?: string): string {
  return trimTrailingSlash(value ?? DEFAULT_FEDERAL_REVENUE_WEBDAV_URL);
}

function getShareToken(value?: string): string {
  const shareToken = value?.trim();

  if (!shareToken) {
    throw new ValidationError(
      "O token do compartilhamento público da Receita Federal não está configurado. Execute `cnpj-db-loader rfb config set share-token <token>` ou passe --share-token.",
    );
  }

  return shareToken;
}

function encodePathSegment(value: string): string {
  return encodeURIComponent(value).replace(/%2F/gi, "/");
}

function decodeXml(value: string): string {
  return value
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

function decodeHrefSegment(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function getAuthHeader(shareToken: string): string {
  return `Basic ${Buffer.from(`${shareToken}:`).toString("base64")}`;
}

function buildUrl(baseUrl: string, segments: string[] = []): string {
  if (segments.length === 0) {
    return `${baseUrl}/`;
  }

  return `${baseUrl}/${segments.map(encodePathSegment).join("/")}`;
}

function extractFirst(block: string, tagName: string): string | undefined {
  const pattern = new RegExp(
    `<(?:[a-zA-Z0-9_-]+:)?${tagName}\\b[^>]*>([\\s\\S]*?)<\\/(?:[a-zA-Z0-9_-]+:)?${tagName}>`,
    "i",
  );
  const match = block.match(pattern);
  return match?.[1] ? decodeXml(match[1].trim()) : undefined;
}

function isCollectionResponse(block: string): boolean {
  return /<(?:[a-zA-Z0-9_-]+:)?collection\b/i.test(block);
}

function getNameFromHref(href: string): string {
  const cleanHref = href.split("?")[0] ?? href;
  const withoutTrailingSlash = cleanHref.replace(/\/+$/g, "");
  const rawName = withoutTrailingSlash.split("/").pop() ?? withoutTrailingSlash;
  return decodeHrefSegment(rawName);
}

type PropfindEntry = {
  href: string;
  name: string;
  isCollection: boolean;
  sizeInBytes?: number | undefined;
  lastModified?: string | undefined;
  etag?: string | undefined;
};

function parsePropfindXml(xml: string): PropfindEntry[] {
  const responseBlocks = xml.match(
    /<(?:[a-zA-Z0-9_-]+:)?response\b[\s\S]*?<\/(?:[a-zA-Z0-9_-]+:)?response>/gi,
  );

  if (!responseBlocks) {
    return [];
  }

  return responseBlocks
    .map<PropfindEntry | undefined>((block) => {
      const href = extractFirst(block, "href");
      if (!href) {
        return undefined;
      }

      const size = extractFirst(block, "getcontentlength");
      const parsedSize = size ? Number.parseInt(size, 10) : undefined;
      const lastModified = extractFirst(block, "getlastmodified");
      const etag = extractFirst(block, "getetag");

      return {
        href,
        name: getNameFromHref(href),
        isCollection: isCollectionResponse(block),
        ...(Number.isFinite(parsedSize) ? { sizeInBytes: parsedSize } : {}),
        ...(lastModified ? { lastModified } : {}),
        ...(etag ? { etag } : {}),
      };
    })
    .filter((entry): entry is PropfindEntry => entry !== undefined);
}

async function propfind(
  pathSegments: string[],
  options: FederalRevenueClientOptions = {},
): Promise<{ entries: PropfindEntry[]; baseUrl: string; shareToken: string }> {
  const baseUrl = normalizeBaseUrl(options.baseUrl);
  const shareToken = getShareToken(options.shareToken);
  let response: Response;

  try {
    response = await fetch(buildUrl(baseUrl, pathSegments), {
      method: "PROPFIND",
      headers: {
        Accept: "application/xml,text/xml,*/*",
        Authorization: getAuthHeader(shareToken),
        Depth: "1",
        "User-Agent": options.userAgent ?? DEFAULT_FEDERAL_REVENUE_USER_AGENT,
      },
    });
  } catch (error) {
    throw new ValidationError(
      `A requisição WebDAV da Receita Federal falhou antes de receber uma resposta: ${error instanceof Error ? error.message : String(error)}.`,
      { baseUrl, pathSegments },
    );
  }

  if (!response.ok) {
    throw new ValidationError(
      `A requisição WebDAV da Receita Federal falhou com status ${response.status} ${response.statusText}.`,
      { status: response.status, statusText: response.statusText },
    );
  }

  const xml = await response.text();
  return {
    entries: parsePropfindXml(xml),
    baseUrl,
    shareToken,
  };
}

export function validateFederalRevenueReference(reference: string): void {
  if (!REFERENCE_PATTERN.test(reference)) {
    throw new ValidationError(
      `Referência da Receita Federal inválida: ${reference}. Esperado YYYY-MM.`,
    );
  }
}

export function getCurrentFederalRevenueReference(date = new Date()): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  return `${year}-${month}`;
}

export async function listFederalRevenueReferences(
  options: FederalRevenueClientOptions = {},
): Promise<{ references: FederalRevenueReference[]; remoteBaseUrl: string }> {
  const result = await propfind([], options);
  const references = result.entries
    .filter((entry) => entry.isCollection && REFERENCE_PATTERN.test(entry.name))
    .map<FederalRevenueReference>((entry) => ({
      reference: entry.name,
      href: entry.href,
    }))
    .sort((left, right) => left.reference.localeCompare(right.reference));

  return {
    references,
    remoteBaseUrl: result.baseUrl,
  };
}

export async function resolveFederalRevenueReference(
  input: {
    reference?: string | undefined;
    current?: boolean | undefined;
  } & FederalRevenueClientOptions = {},
): Promise<FederalRevenueReferenceSelection> {
  const { references } = await listFederalRevenueReferences(input);
  const availableReferences = references.map((item) => item.reference);
  const latest = availableReferences.at(-1);

  if (!latest) {
    throw new ValidationError(
      "A descoberta de referências da Receita Federal falhou: nenhuma referência mensal foi encontrada no compartilhamento público.",
    );
  }

  if (input.reference) {
    validateFederalRevenueReference(input.reference);

    if (!availableReferences.includes(input.reference)) {
      throw new ValidationError(
        `Referência da Receita Federal não encontrada: ${input.reference}. A última referência disponível é ${latest}.`,
        {
          requestedReference: input.reference,
          latestAvailableReference: latest,
          availableReferences,
        },
      );
    }

    return {
      mode: "explicit",
      selectedReference: input.reference,
      availableReferences,
    };
  }

  if (input.current) {
    const currentReference = getCurrentFederalRevenueReference();

    if (!availableReferences.includes(currentReference)) {
      throw new ValidationError(
        `A referência atual da Receita Federal ainda não está disponível: ${currentReference}. A última referência disponível é ${latest}.`,
        {
          requestedReference: currentReference,
          latestAvailableReference: latest,
          availableReferences,
        },
      );
    }

    return {
      mode: "current",
      selectedReference: currentReference,
      availableReferences,
    };
  }

  return {
    mode: "latest",
    selectedReference: latest,
    availableReferences,
  };
}

export async function listFederalRevenueFiles(
  reference: string,
  options: FederalRevenueClientOptions = {},
): Promise<{ files: FederalRevenueFile[]; remoteBaseUrl: string }> {
  validateFederalRevenueReference(reference);
  const result = await propfind([reference], options);
  const files = result.entries
    .filter(
      (entry) =>
        !entry.isCollection && entry.name.toLowerCase().endsWith(".zip"),
    )
    .map<FederalRevenueFile>((entry) => ({
      name: entry.name,
      href: entry.href,
      downloadUrl: buildUrl(result.baseUrl, [reference, entry.name]),
      ...(entry.sizeInBytes !== undefined
        ? { sizeInBytes: entry.sizeInBytes }
        : {}),
      ...(entry.lastModified ? { lastModified: entry.lastModified } : {}),
      ...(entry.etag ? { etag: entry.etag } : {}),
    }))
    .sort((left, right) => left.name.localeCompare(right.name));

  return {
    files,
    remoteBaseUrl: result.baseUrl,
  };
}

export function buildFederalRevenueDownloadHeaders(
  options: FederalRevenueClientOptions = {},
): Record<string, string> {
  return {
    Authorization: getAuthHeader(getShareToken(options.shareToken)),
    "User-Agent": options.userAgent ?? DEFAULT_FEDERAL_REVENUE_USER_AGENT,
  };
}
